package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
)

// MemoryStore is an in-memory implementation of the Store interface.
// Suitable for development and single-node deployments.
type MemoryStore struct {
	mu              sync.RWMutex
	tasks           map[string]*proto.TaskInstance
	workers         map[string]*proto.WorkerInfo
	retentionPeriod time.Duration
}

// NewMemoryStore creates a new in-memory store with the given retention period.
func NewMemoryStore(retention time.Duration) *MemoryStore {
	if retention <= 0 {
		retention = 168 * time.Hour
	}
	return &MemoryStore{
		tasks:           make(map[string]*proto.TaskInstance),
		workers:         make(map[string]*proto.WorkerInfo),
		retentionPeriod: retention,
	}
}

// SaveTask stores or overwrites a task instance.
func (m *MemoryStore) SaveTask(_ context.Context, task *proto.TaskInstance) error {
	if task == nil || task.ID == "" {
		return fmt.Errorf("task must have a non-empty ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy to prevent caller mutations from corrupting stored data.
	stored := *task
	if task.Payload != nil {
		stored.Payload = make([]byte, len(task.Payload))
		copy(stored.Payload, task.Payload)
	}
	if task.Dependencies != nil {
		stored.Dependencies = make([]string, len(task.Dependencies))
		copy(stored.Dependencies, task.Dependencies)
	}
	m.tasks[task.ID] = &stored
	return nil
}

// GetTask retrieves a task by ID. Returns an error if not found.
func (m *MemoryStore) GetTask(_ context.Context, id string) (*proto.TaskInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("task %s not found", id)
	}

	cp := *task
	return &cp, nil
}

// DeleteTask removes a task from storage.
func (m *MemoryStore) DeleteTask(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tasks[id]; !ok {
		return fmt.Errorf("task %s not found", id)
	}
	delete(m.tasks, id)
	return nil
}

// ListTasks returns tasks matching the filter criteria.
func (m *MemoryStore) ListTasks(_ context.Context, filter TaskFilter) ([]*proto.TaskInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*proto.TaskInstance
	for _, task := range m.tasks {
		if filter.State != "" && task.State != filter.State {
			continue
		}
		if filter.AssignedWorker != "" && task.AssignedWorker != filter.AssignedWorker {
			continue
		}
		cp := *task
		result = append(result, &cp)
	}

	// Stable sort by creation time so results are deterministic.
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})

	if filter.Offset > 0 {
		if filter.Offset >= len(result) {
			return nil, nil
		}
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}

	return result, nil
}

// UpdateTaskState transitions a task to a new state. Validates the transition
// against the state machine before applying.
func (m *MemoryStore) UpdateTaskState(_ context.Context, id string, state proto.TaskState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}

	if !proto.CanTransition(task.State, state) {
		return fmt.Errorf("invalid transition from %s to %s for task %s", task.State, state, id)
	}

	task.State = state
	switch state {
	case proto.TaskStateRunning:
		task.StartedAt = time.Now()
	case proto.TaskStateCompleted, proto.TaskStateFailed, proto.TaskStateCancelled:
		task.CompletedAt = time.Now()
	case proto.TaskStateRetrying:
		task.RetryCount++
	}

	return nil
}

// AssignTask sets the worker for a task and transitions it to running.
func (m *MemoryStore) AssignTask(_ context.Context, taskID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.State != proto.TaskStatePending && task.State != proto.TaskStateRetrying {
		return fmt.Errorf("cannot assign task %s in state %s", taskID, task.State)
	}

	task.AssignedWorker = workerID
	task.State = proto.TaskStateRunning
	task.StartedAt = time.Now()
	return nil
}

// CountByState returns the number of tasks in each state.
func (m *MemoryStore) CountByState(_ context.Context) (map[proto.TaskState]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := make(map[proto.TaskState]int)
	for _, task := range m.tasks {
		counts[task.State]++
	}
	return counts, nil
}

// SaveWorker stores or overwrites worker info.
func (m *MemoryStore) SaveWorker(_ context.Context, worker *proto.WorkerInfo) error {
	if worker == nil || worker.ID == "" {
		return fmt.Errorf("worker must have a non-empty ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := *worker
	if worker.Tags != nil {
		cp.Tags = make([]string, len(worker.Tags))
		copy(cp.Tags, worker.Tags)
	}
	m.workers[worker.ID] = &cp
	return nil
}

// GetWorker retrieves a worker by ID.
func (m *MemoryStore) GetWorker(_ context.Context, id string) (*proto.WorkerInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	w, ok := m.workers[id]
	if !ok {
		return nil, fmt.Errorf("worker %s not found", id)
	}
	cp := *w
	return &cp, nil
}

// ListWorkers returns all registered workers.
func (m *MemoryStore) ListWorkers(_ context.Context) ([]*proto.WorkerInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*proto.WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		cp := *w
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// DeleteWorker removes a worker from storage.
func (m *MemoryStore) DeleteWorker(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.workers[id]; !ok {
		return fmt.Errorf("worker %s not found", id)
	}
	delete(m.workers, id)
	return nil
}

// GarbageCollect removes completed, failed, and cancelled tasks older than
// the retention period. Returns the number of removed tasks.
func (m *MemoryStore) GarbageCollect(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.retentionPeriod)
	removed := 0

	for id, task := range m.tasks {
		switch task.State {
		case proto.TaskStateCompleted, proto.TaskStateFailed, proto.TaskStateCancelled:
			if !task.CompletedAt.IsZero() && task.CompletedAt.Before(cutoff) {
				delete(m.tasks, id)
				removed++
			}
		}
	}

	return removed, nil
}

// Close is a no-op for the in-memory store.
func (m *MemoryStore) Close() error {
	return nil
}

// TaskCount returns the total number of stored tasks. Useful for testing.
func (m *MemoryStore) TaskCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tasks)
}

// WorkerCount returns the total number of stored workers. Useful for testing.
func (m *MemoryStore) WorkerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.workers)
}
