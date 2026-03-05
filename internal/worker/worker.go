package worker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/config"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

// Worker represents a task execution node that registers with the scheduler
// and processes assigned tasks concurrently.
type Worker struct {
	mu            sync.RWMutex
	id            string
	cfg           *config.Config
	log           *logger.Logger
	schedulerAddr string
	listenAddr    string
	executor      *Executor
	health        *HealthReporter
	listener      net.Listener
	activeTasks   int32
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// WorkerService exposes RPC methods called by the scheduler.
type WorkerService struct {
	worker *Worker
}

// NewWorker creates a new Worker node.
func NewWorker(id string, cfg *config.Config, log *logger.Logger, schedulerAddr, listenAddr string) *Worker {
	w := &Worker{
		id:            id,
		cfg:           cfg,
		log:           log,
		schedulerAddr: schedulerAddr,
		listenAddr:    listenAddr,
		stopCh:        make(chan struct{}),
	}

	w.executor = NewExecutor(cfg.Worker.Concurrency, cfg.Worker.TaskTimeout, log)
	w.health = NewHealthReporter(id, schedulerAddr, cfg.Worker.HealthCheckInterval, log)

	return w
}

// Start registers with the scheduler and begins accepting tasks.
func (w *Worker) Start(ctx context.Context) error {
	svc := &WorkerService{worker: w}
	server := rpc.NewServer()
	if err := server.Register(svc); err != nil {
		return fmt.Errorf("register worker RPC: %w", err)
	}

	ln, err := net.Listen("tcp", w.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", w.listenAddr, err)
	}
	w.listener = ln
	w.log.Info("worker %s listening on %s", w.id, w.listenAddr)

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)
	mux.Handle(rpc.DefaultDebugPath, rpc.DefaultServer)

	go func() {
		srv := &http.Server{Handler: mux}
		if err := srv.Serve(ln); err != nil {
			select {
			case <-w.stopCh:
			default:
				w.log.Error("worker http serve: %v", err)
			}
		}
	}()

	if err := w.register(); err != nil {
		return fmt.Errorf("register with scheduler: %w", err)
	}

	w.wg.Add(1)
	go w.health.Start(w.stopCh, &w.wg, &w.activeTasks)

	<-ctx.Done()
	return w.Shutdown()
}

// register contacts the scheduler to announce this worker.
func (w *Worker) register() error {
	var lastErr error
	for attempt := 0; attempt < w.cfg.Worker.MaxRegisterRetries; attempt++ {
		client, err := rpc.DialHTTP("tcp", w.schedulerAddr)
		if err != nil {
			lastErr = err
			w.log.Warn("register attempt %d failed (dial): %v", attempt+1, err)
			time.Sleep(w.cfg.Worker.RegisterRetryInterval)
			continue
		}

		req := &proto.RegisterWorkerRequest{
			WorkerID:    w.id,
			Address:     w.listenAddr,
			Concurrency: w.cfg.Worker.Concurrency,
		}
		var resp proto.RegisterWorkerResponse

		err = client.Call("SchedulerService.RegisterWorker", req, &resp)
		client.Close()
		if err != nil {
			lastErr = err
			w.log.Warn("register attempt %d failed (call): %v", attempt+1, err)
			time.Sleep(w.cfg.Worker.RegisterRetryInterval)
			continue
		}

		if resp.Accepted {
			w.log.Info("registered with scheduler at %s", w.schedulerAddr)
			return nil
		}

		lastErr = fmt.Errorf("registration rejected: %s", resp.Message)
		w.log.Warn("register attempt %d rejected: %s", attempt+1, resp.Message)
		time.Sleep(w.cfg.Worker.RegisterRetryInterval)
	}

	return fmt.Errorf("failed to register after %d attempts: %w", w.cfg.Worker.MaxRegisterRetries, lastErr)
}

// Shutdown gracefully stops the worker, waiting for in-flight tasks.
func (w *Worker) Shutdown() error {
	w.log.Info("shutting down worker %s", w.id)
	close(w.stopCh)

	if w.listener != nil {
		w.listener.Close()
	}

	w.executor.Drain()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.log.Info("worker %s shutdown complete", w.id)
	case <-time.After(w.cfg.Worker.ShutdownGracePeriod):
		w.log.Warn("worker %s shutdown timed out", w.id)
	}

	return nil
}

// ExecuteTask is called by the scheduler to run a task on this worker.
func (svc *WorkerService) ExecuteTask(req *proto.ExecuteTaskRequest, resp *proto.ExecuteTaskResponse) error {
	w := svc.worker

	active := atomic.AddInt32(&w.activeTasks, 1)
	w.log.Info("executing task %s (active=%d)", req.Task.ID, active)

	result, err := w.executor.Execute(context.Background(), req.Task, req.Timeout)

	atomic.AddInt32(&w.activeTasks, -1)

	if err != nil {
		resp.TaskID = req.Task.ID
		resp.Success = false
		resp.Error = err.Error()
		return nil
	}

	resp.TaskID = result.TaskID
	resp.Success = result.Success
	resp.Result = result.Output
	resp.Error = result.Error
	resp.Duration = result.Duration
	resp.ExitCode = result.ExitCode
	return nil
}

// GetStatus returns the current worker status.
func (svc *WorkerService) GetStatus(req *struct{}, resp *proto.WorkerInfo) error {
	w := svc.worker
	resp.ID = w.id
	resp.Address = w.listenAddr
	resp.Concurrency = w.cfg.Worker.Concurrency
	resp.ActiveTasks = int(atomic.LoadInt32(&w.activeTasks))
	resp.Healthy = true
	resp.LastHeartbeat = time.Now()
	return nil
}
