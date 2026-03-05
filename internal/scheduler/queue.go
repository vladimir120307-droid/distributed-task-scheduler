package scheduler

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
)

// priorityItem wraps a TaskInstance for use in a heap-based priority queue.
type priorityItem struct {
	task    *proto.TaskInstance
	index   int
	enqueue time.Time
}

// priorityHeap implements heap.Interface for priorityItem elements.
// Higher priority value means higher urgency. Among equal priorities,
// earlier enqueue time wins.
type priorityHeap []*priorityItem

func (h priorityHeap) Len() int { return len(h) }

func (h priorityHeap) Less(i, j int) bool {
	if h[i].task.Priority != h[j].task.Priority {
		return h[i].task.Priority > h[j].task.Priority
	}
	return h[i].enqueue.Before(h[j].enqueue)
}

func (h priorityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *priorityHeap) Push(x interface{}) {
	item := x.(*priorityItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *priorityHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// TaskQueue is a thread-safe priority queue for scheduling tasks.
type TaskQueue struct {
	mu      sync.Mutex
	heap    priorityHeap
	items   map[string]*priorityItem
	maxSize int
}

// NewTaskQueue creates a new TaskQueue with the given capacity. A maxSize
// of zero or negative means unlimited.
func NewTaskQueue(maxSize int) *TaskQueue {
	q := &TaskQueue{
		heap:    make(priorityHeap, 0),
		items:   make(map[string]*priorityItem),
		maxSize: maxSize,
	}
	heap.Init(&q.heap)
	return q
}

// Enqueue inserts a task into the priority queue. Returns an error if the
// queue is full or the task is already enqueued.
func (q *TaskQueue) Enqueue(task *proto.TaskInstance) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.items[task.ID]; exists {
		return fmt.Errorf("task %s already in queue", task.ID)
	}

	if q.maxSize > 0 && len(q.heap) >= q.maxSize {
		return fmt.Errorf("queue is full (max %d)", q.maxSize)
	}

	item := &priorityItem{
		task:    task,
		enqueue: time.Now(),
	}
	heap.Push(&q.heap, item)
	q.items[task.ID] = item
	return nil
}

// Dequeue removes and returns the highest-priority task. Returns nil if the
// queue is empty.
func (q *TaskQueue) Dequeue() *proto.TaskInstance {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.heap) == 0 {
		return nil
	}

	item := heap.Pop(&q.heap).(*priorityItem)
	delete(q.items, item.task.ID)
	return item.task
}

// Peek returns the highest-priority task without removing it. Returns nil
// if the queue is empty.
func (q *TaskQueue) Peek() *proto.TaskInstance {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.heap) == 0 {
		return nil
	}
	return q.heap[0].task
}

// Remove removes a specific task from the queue by ID. Returns false if the
// task was not found.
func (q *TaskQueue) Remove(taskID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	item, exists := q.items[taskID]
	if !exists {
		return false
	}

	heap.Remove(&q.heap, item.index)
	delete(q.items, taskID)
	return true
}

// UpdatePriority changes the priority of a task already in the queue.
func (q *TaskQueue) UpdatePriority(taskID string, newPriority int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	item, exists := q.items[taskID]
	if !exists {
		return fmt.Errorf("task %s not in queue", taskID)
	}

	item.task.Priority = newPriority
	heap.Fix(&q.heap, item.index)
	return nil
}

// Len returns the number of tasks in the queue.
func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.heap)
}

// Contains checks whether a task is currently in the queue.
func (q *TaskQueue) Contains(taskID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, exists := q.items[taskID]
	return exists
}

// Drain removes and returns all tasks from the queue, ordered by priority.
func (q *TaskQueue) Drain() []*proto.TaskInstance {
	q.mu.Lock()
	defer q.mu.Unlock()

	tasks := make([]*proto.TaskInstance, 0, len(q.heap))
	for len(q.heap) > 0 {
		item := heap.Pop(&q.heap).(*priorityItem)
		delete(q.items, item.task.ID)
		tasks = append(tasks, item.task)
	}
	return tasks
}

// DequeueBatch removes up to n tasks from the queue in priority order.
func (q *TaskQueue) DequeueBatch(n int) []*proto.TaskInstance {
	q.mu.Lock()
	defer q.mu.Unlock()

	if n <= 0 || len(q.heap) == 0 {
		return nil
	}
	if n > len(q.heap) {
		n = len(q.heap)
	}

	tasks := make([]*proto.TaskInstance, 0, n)
	for i := 0; i < n; i++ {
		item := heap.Pop(&q.heap).(*priorityItem)
		delete(q.items, item.task.ID)
		tasks = append(tasks, item.task)
	}
	return tasks
}
