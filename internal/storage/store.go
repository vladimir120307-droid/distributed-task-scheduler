package storage

import (
	"context"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
)

// TaskFilter specifies optional criteria for listing tasks.
type TaskFilter struct {
	State          proto.TaskState
	AssignedWorker string
	Limit          int
	Offset         int
}

// Store defines the interface for persisting task state. Implementations
// must be safe for concurrent access.
type Store interface {
	// SaveTask persists a task instance. If the task already exists, it is updated.
	SaveTask(ctx context.Context, task *proto.TaskInstance) error

	// GetTask retrieves a task by its instance ID.
	GetTask(ctx context.Context, id string) (*proto.TaskInstance, error)

	// DeleteTask removes a task from storage.
	DeleteTask(ctx context.Context, id string) error

	// ListTasks returns tasks matching the filter. An empty filter returns all tasks.
	ListTasks(ctx context.Context, filter TaskFilter) ([]*proto.TaskInstance, error)

	// UpdateTaskState atomically transitions a task to a new state. Returns an error
	// if the transition is invalid according to the state machine.
	UpdateTaskState(ctx context.Context, id string, state proto.TaskState) error

	// AssignTask sets the worker for a task and transitions it to running.
	AssignTask(ctx context.Context, taskID, workerID string) error

	// CountByState returns a mapping of state to count for all stored tasks.
	CountByState(ctx context.Context) (map[proto.TaskState]int, error)

	// SaveWorker persists worker info.
	SaveWorker(ctx context.Context, worker *proto.WorkerInfo) error

	// GetWorker retrieves a worker by ID.
	GetWorker(ctx context.Context, id string) (*proto.WorkerInfo, error)

	// ListWorkers returns all known workers.
	ListWorkers(ctx context.Context) ([]*proto.WorkerInfo, error)

	// DeleteWorker removes a worker from storage.
	DeleteWorker(ctx context.Context, id string) error

	// GarbageCollect removes completed/failed tasks older than the retention period.
	GarbageCollect(ctx context.Context) (int, error)

	// Close releases any resources held by the store.
	Close() error
}
