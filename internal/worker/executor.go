package worker

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

// TaskResult holds the outcome of a task execution.
type TaskResult struct {
	TaskID   string
	Success  bool
	Output   []byte
	Error    string
	Duration time.Duration
	ExitCode int
}

// Executor manages concurrent task execution with a bounded worker pool.
type Executor struct {
	mu          sync.Mutex
	concurrency int
	timeout     time.Duration
	log         *logger.Logger
	sem         chan struct{}
	running     map[string]context.CancelFunc
	wg          sync.WaitGroup
}

// NewExecutor creates an Executor with the given concurrency limit and
// default per-task timeout.
func NewExecutor(concurrency int, timeout time.Duration, log *logger.Logger) *Executor {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Executor{
		concurrency: concurrency,
		timeout:     timeout,
		log:         log,
		sem:         make(chan struct{}, concurrency),
		running:     make(map[string]context.CancelFunc),
	}
}

// Execute runs a task synchronously within the executor pool. It blocks until
// a concurrency slot is available, then executes the task with a timeout.
func (e *Executor) Execute(ctx context.Context, task *proto.TaskInstance, timeout time.Duration) (*TaskResult, error) {
	if timeout <= 0 {
		timeout = e.timeout
	}

	// Acquire semaphore slot.
	select {
	case e.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-e.sem }()

	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	e.mu.Lock()
	e.running[task.ID] = cancel
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.running, task.ID)
		e.mu.Unlock()
	}()

	e.wg.Add(1)
	defer e.wg.Done()

	start := time.Now()
	result := e.executeTask(taskCtx, task)
	result.Duration = time.Since(start)

	return result, nil
}

// executeTask contains the actual task execution logic. In a production system
// this would dispatch to registered task handlers based on the task name.
func (e *Executor) executeTask(ctx context.Context, task *proto.TaskInstance) *TaskResult {
	result := &TaskResult{
		TaskID: task.ID,
	}

	e.log.Debug("starting execution of task %s (%s)", task.ID, task.Name)

	// Simulate task execution with variable duration.
	// In production, this dispatches to a handler registry.
	duration := time.Duration(100+rand.Intn(900)) * time.Millisecond

	select {
	case <-time.After(duration):
		// Task completed successfully.
		result.Success = true
		result.Output = []byte(fmt.Sprintf("task %s completed by executor", task.ID))
		result.ExitCode = 0
		e.log.Debug("task %s finished successfully", task.ID)

	case <-ctx.Done():
		result.Success = false
		result.Error = fmt.Sprintf("task %s timed out or was cancelled: %v", task.ID, ctx.Err())
		result.ExitCode = -1
		e.log.Warn("task %s cancelled/timed out", task.ID)
	}

	return result
}

// CancelTask cancels a running task by its ID.
func (e *Executor) CancelTask(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	cancel, ok := e.running[taskID]
	if !ok {
		return false
	}

	cancel()
	e.log.Info("task %s cancellation requested", taskID)
	return true
}

// RunningTasks returns the IDs of all currently executing tasks.
func (e *Executor) RunningTasks() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	ids := make([]string, 0, len(e.running))
	for id := range e.running {
		ids = append(ids, id)
	}
	return ids
}

// ActiveCount returns the number of currently executing tasks.
func (e *Executor) ActiveCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.running)
}

// Drain waits for all running tasks to complete.
func (e *Executor) Drain() {
	e.log.Info("draining executor, waiting for %d tasks", e.ActiveCount())
	e.wg.Wait()
	e.log.Info("executor drain complete")
}

// AvailableSlots returns the number of free concurrency slots.
func (e *Executor) AvailableSlots() int {
	return e.concurrency - e.ActiveCount()
}
