package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/hash"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/storage"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/config"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

// Scheduler is the central coordination service.
type Scheduler struct {
	mu       sync.RWMutex
	cfg      *config.Config
	log      *logger.Logger
	store    storage.Store
	ring     *hash.Ring
	queue    *TaskQueue
	elector  *LeaderElector
	listener net.Listener
	workers  map[string]*proto.WorkerInfo
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// SchedulerService exposes RPC methods to clients and workers.
type SchedulerService struct {
	sched *Scheduler
}

// NewScheduler constructs a Scheduler from configuration.
func NewScheduler(cfg *config.Config, log *logger.Logger, store storage.Store) *Scheduler {
	s := &Scheduler{
		cfg:     cfg,
		log:     log,
		store:   store,
		ring:    hash.NewRing(cfg.HashRing.VirtualNodes, cfg.HashRing.ReplicationFactor),
		queue:   NewTaskQueue(cfg.Scheduler.MaxTasksInFlight),
		workers: make(map[string]*proto.WorkerInfo),
		stopCh:  make(chan struct{}),
	}

	s.elector = NewLeaderElector(ElectorConfig{
		NodeID:          fmt.Sprintf("sched-%d", time.Now().UnixNano()),
		ListenAddr:      cfg.Scheduler.ListenAddr,
		HeartbeatTick:   cfg.Scheduler.HeartbeatInterval,
		ElectionTimeout: cfg.Scheduler.ElectionTimeout,
		Log:             log,
		OnBecomeLeader:  s.onBecomeLeader,
		OnLoseLeader:    s.onLoseLeader,
	})

	return s
}

func (s *Scheduler) onBecomeLeader() {
	s.log.Info("this node is now the leader, starting dispatch loop")
	s.wg.Add(1)
	go s.dispatchLoop()
}

func (s *Scheduler) onLoseLeader() {
	s.log.Warn("lost leadership, stopping dispatch")
}

// Start initializes the RPC server and begins serving.
func (s *Scheduler) Start(ctx context.Context) error {
	svc := &SchedulerService{sched: s}
	if err := rpc.Register(svc); err != nil {
		return fmt.Errorf("register RPC service: %w", err)
	}
	rpc.HandleHTTP()

	ln, err := net.Listen("tcp", s.cfg.Scheduler.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Scheduler.ListenAddr, err)
	}
	s.listener = ln
	s.log.Info("scheduler listening on %s", s.cfg.Scheduler.ListenAddr)

	s.elector.Start()
	s.wg.Add(2)
	go s.healthCheckLoop()
	go s.gcLoop()

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			select {
			case <-s.stopCh:
			default:
				s.log.Error("http serve error: %v", err)
			}
		}
	}()

	<-ctx.Done()
	return s.Shutdown()
}

// Shutdown performs a graceful shutdown.
func (s *Scheduler) Shutdown() error {
	s.log.Info("shutting down scheduler")
	close(s.stopCh)
	s.elector.Stop()
	if s.listener != nil {
		s.listener.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.log.Info("scheduler shutdown complete")
	case <-time.After(s.cfg.Scheduler.DrainTimeout):
		s.log.Warn("scheduler shutdown timed out")
	}
	return s.store.Close()
}

// SubmitTask handles task submission from clients.
func (svc *SchedulerService) SubmitTask(req *proto.SubmitTaskRequest, resp *proto.SubmitTaskResponse) error {
	if req.Task == nil {
		return fmt.Errorf("task definition must not be nil")
	}
	s := svc.sched
	taskID := req.Task.ID
	if taskID == "" {
		taskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	maxRetries := req.Task.MaxRetries
	if maxRetries == 0 {
		maxRetries = s.cfg.Retry.MaxRetries
	}
	instance := &proto.TaskInstance{
		ID:           taskID,
		DefinitionID: req.Task.ID,
		Name:         req.Task.Name,
		Payload:      req.Task.Payload,
		Priority:     req.Task.Priority,
		State:        proto.TaskStatePending,
		MaxRetries:   maxRetries,
		CreatedAt:    time.Now(),
		Dependencies: req.Task.Dependencies,
		CronExpr:     req.Task.CronExpr,
	}
	ctx := context.Background()
	if err := s.store.SaveTask(ctx, instance); err != nil {
		return fmt.Errorf("save task: %w", err)
	}
	if err := s.queue.Enqueue(instance); err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}
	s.log.Info("task %s submitted (priority=%d)", taskID, instance.Priority)
	resp.TaskID = taskID
	resp.Status = string(proto.TaskStatePending)
	return nil
}

// SubmitDAG handles submission of a directed acyclic graph of tasks.
func (svc *SchedulerService) SubmitDAG(req *proto.SubmitDAGRequest, resp *proto.SubmitDAGResponse) error {
	s := svc.sched
	if err := validateDAG(req.Tasks); err != nil {
		return fmt.Errorf("invalid DAG: %w", err)
	}
	ctx := context.Background()
	taskIDs := make([]string, 0, len(req.Tasks))
	for _, def := range req.Tasks {
		taskID := def.ID
		if taskID == "" {
			taskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
		}
		instance := &proto.TaskInstance{
			ID:           taskID,
			DefinitionID: def.ID,
			Name:         def.Name,
			Payload:      def.Payload,
			Priority:     def.Priority,
			State:        proto.TaskStatePending,
			MaxRetries:   def.MaxRetries,
			CreatedAt:    time.Now(),
			Dependencies: def.Dependencies,
			CronExpr:     def.CronExpr,
		}
		if err := s.store.SaveTask(ctx, instance); err != nil {
			return fmt.Errorf("save DAG task %s: %w", taskID, err)
		}
		if len(def.Dependencies) == 0 {
			if err := s.queue.Enqueue(instance); err != nil {
				s.log.Warn("failed to enqueue root task %s: %v", taskID, err)
			}
		}
		taskIDs = append(taskIDs, taskID)
	}
	s.log.Info("DAG submitted with %d tasks", len(taskIDs))
	resp.TaskIDs = taskIDs
	resp.Status = "accepted"
	return nil
}

// GetTaskStatus returns the current state of a task.
func (svc *SchedulerService) GetTaskStatus(req *proto.TaskStatusRequest, resp *proto.TaskStatusResponse) error {
	task, err := svc.sched.store.GetTask(context.Background(), req.TaskID)
	if err != nil {
		return err
	}
	resp.TaskID = task.ID
	resp.State = task.State
	resp.AssignedWorker = task.AssignedWorker
	resp.RetryCount = task.RetryCount
	resp.Error = task.Error
	resp.CreatedAt = task.CreatedAt
	resp.StartedAt = task.StartedAt
	resp.CompletedAt = task.CompletedAt
	return nil
}

// CancelTask marks a task as cancelled.
func (svc *SchedulerService) CancelTask(req *proto.CancelTaskRequest, resp *proto.CancelTaskResponse) error {
	s := svc.sched
	ctx := context.Background()
	err := s.store.UpdateTaskState(ctx, req.TaskID, proto.TaskStateCancelled)
	if err != nil {
		resp.Success = false
		resp.Message = err.Error()
		return nil
	}
	s.queue.Remove(req.TaskID)
	s.log.Info("task %s cancelled", req.TaskID)
	resp.Success = true
	resp.Message = "cancelled"
	return nil
}

// RegisterWorker adds a worker to the cluster.
func (svc *SchedulerService) RegisterWorker(req *proto.RegisterWorkerRequest, resp *proto.RegisterWorkerResponse) error {
	s := svc.sched
	worker := &proto.WorkerInfo{
		ID:            req.WorkerID,
		Address:       req.Address,
		Concurrency:   req.Concurrency,
		ActiveTasks:   0,
		LastHeartbeat: time.Now(),
		Healthy:       true,
		Tags:          req.Tags,
	}
	s.mu.Lock()
	s.workers[req.WorkerID] = worker
	s.ring.AddNode(req.WorkerID)
	s.mu.Unlock()
	if err := s.store.SaveWorker(context.Background(), worker); err != nil {
		s.log.Error("failed to persist worker %s: %v", req.WorkerID, err)
	}
	s.log.Info("worker %s registered at %s (concurrency=%d)", req.WorkerID, req.Address, req.Concurrency)
	resp.Accepted = true
	resp.LeaderAddr = s.elector.LeaderAddr()
	resp.Message = "registered"
	return nil
}

// Heartbeat processes a worker heartbeat signal.
func (svc *SchedulerService) Heartbeat(req *proto.HeartbeatRequest, resp *proto.HeartbeatResponse) error {
	s := svc.sched
	s.mu.Lock()
	if w, ok := s.workers[req.WorkerID]; ok {
		w.LastHeartbeat = req.Timestamp
		w.ActiveTasks = req.ActiveTasks
		w.Healthy = true
	}
	s.mu.Unlock()
	resp.Acknowledged = true
	return nil
}

// GetClusterStatus returns high-level cluster metrics.
func (svc *SchedulerService) GetClusterStatus(req *proto.ClusterStatusRequest, resp *proto.ClusterStatusResponse) error {
	s := svc.sched
	ctx := context.Background()
	counts, err := s.store.CountByState(ctx)
	if err != nil {
		return err
	}
	s.mu.RLock()
	healthy := 0
	for _, w := range s.workers {
		if w.Healthy {
			healthy++
		}
	}
	totalWorkers := len(s.workers)
	s.mu.RUnlock()
	resp.LeaderID = s.elector.LeaderID()
	resp.TotalWorkers = totalWorkers
	resp.HealthyWorkers = healthy
	resp.PendingTasks = counts[proto.TaskStatePending]
	resp.RunningTasks = counts[proto.TaskStateRunning]
	resp.CompletedTasks = counts[proto.TaskStateCompleted]
	resp.FailedTasks = counts[proto.TaskStateFailed]
	return nil
}

func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if !s.elector.IsLeader() {
				continue
			}
			s.dispatchBatch()
		}
	}
}

func (s *Scheduler) dispatchBatch() {
	tasks := s.queue.DequeueBatch(10)
	for _, task := range tasks {
		if err := s.assignTask(task); err != nil {
			s.log.Error("failed to assign task %s: %v", task.ID, err)
			s.queue.Enqueue(task)
		}
	}
}

func (s *Scheduler) assignTask(task *proto.TaskInstance) error {
	workerID := s.ring.GetNode(task.ID)
	if workerID == "" {
		return fmt.Errorf("no workers available")
	}
	s.mu.RLock()
	worker, ok := s.workers[workerID]
	s.mu.RUnlock()
	if !ok || !worker.Healthy {
		workerID = s.ring.GetLeastLoadedNode()
		if workerID == "" {
			return fmt.Errorf("no healthy workers")
		}
		s.mu.RLock()
		worker = s.workers[workerID]
		s.mu.RUnlock()
	}
	if err := s.store.AssignTask(context.Background(), task.ID, workerID); err != nil {
		return err
	}
	s.ring.IncrementLoad(workerID)
	execReq := &proto.ExecuteTaskRequest{
		Task:    task,
		Timeout: s.cfg.Worker.TaskTimeout,
	}
	var execResp proto.ExecuteTaskResponse
	client, err := rpc.DialHTTP("tcp", worker.Address)
	if err != nil {
		return fmt.Errorf("dial worker %s: %w", workerID, err)
	}
	defer client.Close()
	go func() {
		err := client.Call("WorkerService.ExecuteTask", execReq, &execResp)
		s.ring.DecrementLoad(workerID)
		if err != nil {
			s.log.Error("task %s exec failed on %s: %v", task.ID, workerID, err)
			s.handleTaskFailure(task)
			return
		}
		s.handleTaskResult(task, &execResp)
	}()
	s.log.Debug("task %s assigned to worker %s", task.ID, workerID)
	return nil
}

func (s *Scheduler) handleTaskResult(task *proto.TaskInstance, resp *proto.ExecuteTaskResponse) {
	ctx := context.Background()
	if resp.Success {
		s.store.UpdateTaskState(ctx, task.ID, proto.TaskStateCompleted)
		s.log.Info("task %s completed in %s", task.ID, resp.Duration)
		s.enqueueDependents(task.ID)
	} else {
		s.log.Warn("task %s failed: %s", task.ID, resp.Error)
		s.handleTaskFailure(task)
	}
}

func (s *Scheduler) handleTaskFailure(task *proto.TaskInstance) {
	ctx := context.Background()
	stored, err := s.store.GetTask(ctx, task.ID)
	if err != nil {
		s.log.Error("cannot get task %s for retry: %v", task.ID, err)
		return
	}
	if stored.RetryCount < stored.MaxRetries {
		s.store.UpdateTaskState(ctx, task.ID, proto.TaskStateRetrying)
		stored.State = proto.TaskStateRetrying
		stored.RetryCount++
		s.queue.Enqueue(stored)
		s.log.Info("task %s retry %d/%d", task.ID, stored.RetryCount, stored.MaxRetries)
	} else {
		s.store.UpdateTaskState(ctx, task.ID, proto.TaskStateFailed)
		s.log.Error("task %s permanently failed after %d retries", task.ID, stored.MaxRetries)
	}
}

func (s *Scheduler) enqueueDependents(completedTaskID string) {
	ctx := context.Background()
	allTasks, err := s.store.ListTasks(ctx, storage.TaskFilter{State: proto.TaskStatePending})
	if err != nil {
		return
	}
	for _, task := range allTasks {
		if !hasDependency(task.Dependencies, completedTaskID) {
			continue
		}
		allDone := true
		for _, depID := range task.Dependencies {
			dep, err := s.store.GetTask(ctx, depID)
			if err != nil || dep.State != proto.TaskStateCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			s.queue.Enqueue(task)
			s.log.Debug("dependent task %s is now runnable", task.ID)
		}
	}
}

func hasDependency(deps []string, id string) bool {
	for _, d := range deps {
		if d == id {
			return true
		}
	}
	return false
}

func (s *Scheduler) healthCheckLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.Worker.HealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkWorkerHealth()
		}
	}
}

func (s *Scheduler) checkWorkerHealth() {
	s.mu.Lock()
	defer s.mu.Unlock()
	threshold := 3 * s.cfg.Worker.HealthCheckInterval
	now := time.Now()
	for id, w := range s.workers {
		if now.Sub(w.LastHeartbeat) > threshold {
			if w.Healthy {
				w.Healthy = false
				s.log.Warn("worker %s unhealthy (no heartbeat for %s)", id, now.Sub(w.LastHeartbeat))
				s.ring.RemoveNode(id)
			}
		}
	}
}

func (s *Scheduler) gcLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.Storage.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			removed, err := s.store.GarbageCollect(context.Background())
			if err != nil {
				s.log.Error("GC failed: %v", err)
			} else if removed > 0 {
				s.log.Info("GC removed %d expired tasks", removed)
			}
		}
	}
}

// validateDAG checks for cycles in the task dependency graph using DFS.
func validateDAG(tasks []*proto.TaskDefinition) error {
	graph := make(map[string][]string)
	taskSet := make(map[string]bool)
	for _, t := range tasks {
		taskSet[t.ID] = true
		graph[t.ID] = t.Dependencies
	}
	for _, t := range tasks {
		for _, dep := range t.Dependencies {
			if !taskSet[dep] {
				return fmt.Errorf("task %s depends on unknown task %s", t.ID, dep)
			}
		}
	}
	visited := make(map[string]int)
	var dfs func(string) error
	dfs = func(node string) error {
		if visited[node] == 1 {
			return fmt.Errorf("cycle detected at task %s", node)
		}
		if visited[node] == 2 {
			return nil
		}
		visited[node] = 1
		for _, dep := range graph[node] {
			if err := dfs(dep); err != nil {
				return err
			}
		}
		visited[node] = 2
		return nil
	}
	for id := range taskSet {
		if err := dfs(id); err != nil {
			return err
		}
	}
	return nil
}
