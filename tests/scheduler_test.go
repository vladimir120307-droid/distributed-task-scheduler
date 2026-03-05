package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/hash"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/scheduler"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/storage"
)

func TestConsistentHashRing_BasicOperations(t *testing.T) {
	ring := hash.NewRing(100, 2)

	if ring.Size() != 0 {
		t.Fatalf("expected empty ring, got size %d", ring.Size())
	}

	ring.AddNode("worker-1")
	ring.AddNode("worker-2")
	ring.AddNode("worker-3")

	if ring.Size() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.Size())
	}

	node := ring.GetNode("some-task-id")
	if node == "" {
		t.Fatal("expected a node for the key, got empty string")
	}

	for i := 0; i < 100; i++ {
		if ring.GetNode("some-task-id") != node {
			t.Fatal("consistent hash returned different node for same key")
		}
	}
}

func TestConsistentHashRing_RemoveNode(t *testing.T) {
	ring := hash.NewRing(50, 1)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	ring.RemoveNode("node-a")

	if ring.Size() != 1 {
		t.Fatalf("expected 1 node after removal, got %d", ring.Size())
	}

	after := ring.GetNode("test-key")
	if after != "node-b" {
		t.Fatalf("expected node-b after removing node-a, got %s", after)
	}
}

func TestConsistentHashRing_GetNodes(t *testing.T) {
	ring := hash.NewRing(50, 3)
	ring.AddNode("n1")
	ring.AddNode("n2")
	ring.AddNode("n3")

	nodes := ring.GetNodes("key-123")
	if len(nodes) != 3 {
		t.Fatalf("expected 3 replica nodes, got %d", len(nodes))
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n] {
			t.Fatalf("duplicate node in replica set: %s", n)
		}
		seen[n] = true
	}
}

func TestConsistentHashRing_Distribution(t *testing.T) {
	ring := hash.NewRing(150, 1)
	ring.AddNode("w1")
	ring.AddNode("w2")
	ring.AddNode("w3")

	dist := ring.Distribution()
	if len(dist) != 3 {
		t.Fatalf("expected distribution across 3 nodes, got %d", len(dist))
	}

	total := 0
	for _, count := range dist {
		total += count
		if count == 0 {
			t.Fatal("node has zero virtual nodes")
		}
	}

	if total != 450 {
		t.Fatalf("expected 450 total vnodes (3*150), got %d", total)
	}
}

func TestConsistentHashRing_LoadTracking(t *testing.T) {
	ring := hash.NewRing(10, 1)
	ring.AddNode("w1")

	ring.IncrementLoad("w1")
	ring.IncrementLoad("w1")
	ring.IncrementLoad("w1")

	if ring.GetLoad("w1") != 3 {
		t.Fatalf("expected load 3, got %d", ring.GetLoad("w1"))
	}

	ring.DecrementLoad("w1")
	if ring.GetLoad("w1") != 2 {
		t.Fatalf("expected load 2, got %d", ring.GetLoad("w1"))
	}

	ring.AddNode("w2")
	least := ring.GetLeastLoadedNode()
	if least != "w2" {
		t.Fatalf("expected w2 as least loaded, got %s", least)
	}
}

func TestPriorityQueue_EnqueueDequeue(t *testing.T) {
	q := scheduler.NewTaskQueue(100)

	tasks := []*proto.TaskInstance{
		{ID: "low", Priority: 1, CreatedAt: time.Now()},
		{ID: "high", Priority: 10, CreatedAt: time.Now()},
		{ID: "medium", Priority: 5, CreatedAt: time.Now()},
	}

	for _, task := range tasks {
		if err := q.Enqueue(task); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	if q.Len() != 3 {
		t.Fatalf("expected queue length 3, got %d", q.Len())
	}

	first := q.Dequeue()
	if first.ID != "high" {
		t.Fatalf("expected highest priority first, got %s", first.ID)
	}

	second := q.Dequeue()
	if second.ID != "medium" {
		t.Fatalf("expected medium priority second, got %s", second.ID)
	}

	third := q.Dequeue()
	if third.ID != "low" {
		t.Fatalf("expected lowest priority third, got %s", third.ID)
	}

	if q.Dequeue() != nil {
		t.Fatal("expected nil from empty queue")
	}
}

func TestPriorityQueue_Remove(t *testing.T) {
	q := scheduler.NewTaskQueue(100)
	q.Enqueue(&proto.TaskInstance{ID: "t1", Priority: 5, CreatedAt: time.Now()})
	q.Enqueue(&proto.TaskInstance{ID: "t2", Priority: 3, CreatedAt: time.Now()})

	if !q.Remove("t1") {
		t.Fatal("expected successful removal of t1")
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 item after removal, got %d", q.Len())
	}
}

func TestPriorityQueue_DuplicateReject(t *testing.T) {
	q := scheduler.NewTaskQueue(100)
	q.Enqueue(&proto.TaskInstance{ID: "dup", Priority: 1, CreatedAt: time.Now()})
	err := q.Enqueue(&proto.TaskInstance{ID: "dup", Priority: 1, CreatedAt: time.Now()})
	if err == nil {
		t.Fatal("expected error for duplicate enqueue")
	}
}

func TestPriorityQueue_MaxSize(t *testing.T) {
	q := scheduler.NewTaskQueue(2)
	q.Enqueue(&proto.TaskInstance{ID: "a", Priority: 1, CreatedAt: time.Now()})
	q.Enqueue(&proto.TaskInstance{ID: "b", Priority: 2, CreatedAt: time.Now()})
	err := q.Enqueue(&proto.TaskInstance{ID: "c", Priority: 3, CreatedAt: time.Now()})
	if err == nil {
		t.Fatal("expected error when exceeding max queue size")
	}
}

func TestPriorityQueue_DequeueBatch(t *testing.T) {
	q := scheduler.NewTaskQueue(100)
	for i := 0; i < 5; i++ {
		q.Enqueue(&proto.TaskInstance{
			ID:        fmt.Sprintf("task-%d", i),
			Priority:  i,
			CreatedAt: time.Now(),
		})
	}
	batch := q.DequeueBatch(3)
	if len(batch) != 3 {
		t.Fatalf("expected batch of 3, got %d", len(batch))
	}
	if q.Len() != 2 {
		t.Fatalf("expected 2 remaining, got %d", q.Len())
	}
}

func TestTaskStateMachine_ValidTransitions(t *testing.T) {
	task := &proto.TaskInstance{ID: "sm-test", State: proto.TaskStatePending}

	if err := task.TransitionTo(proto.TaskStateRunning); err != nil {
		t.Fatalf("pending->running should be valid: %v", err)
	}
	if err := task.TransitionTo(proto.TaskStateCompleted); err != nil {
		t.Fatalf("running->completed should be valid: %v", err)
	}
}

func TestTaskStateMachine_InvalidTransitions(t *testing.T) {
	task := &proto.TaskInstance{ID: "sm-test", State: proto.TaskStatePending}

	if err := task.TransitionTo(proto.TaskStateCompleted); err == nil {
		t.Fatal("pending->completed should be invalid")
	}

	task.State = proto.TaskStateCompleted
	if err := task.TransitionTo(proto.TaskStateRunning); err == nil {
		t.Fatal("completed->running should be invalid")
	}
}

func TestMemoryStore_TaskCRUD(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	task := &proto.TaskInstance{
		ID:        "crud-test",
		Name:      "test-task",
		State:     proto.TaskStatePending,
		Priority:  5,
		CreatedAt: time.Now(),
	}

	if err := store.SaveTask(ctx, task); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := store.GetTask(ctx, "crud-test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "test-task" {
		t.Fatalf("expected name test-task, got %s", got.Name)
	}

	if err := store.DeleteTask(ctx, "crud-test"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = store.GetTask(ctx, "crud-test")
	if err == nil {
		t.Fatal("expected error getting deleted task")
	}
}

func TestMemoryStore_StateTransition(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	task := &proto.TaskInstance{ID: "state-test", State: proto.TaskStatePending, CreatedAt: time.Now()}
	store.SaveTask(ctx, task)

	if err := store.UpdateTaskState(ctx, "state-test", proto.TaskStateRunning); err != nil {
		t.Fatalf("valid transition failed: %v", err)
	}

	err := store.UpdateTaskState(ctx, "state-test", proto.TaskStatePending)
	if err == nil {
		t.Fatal("invalid transition should fail")
	}
}

func TestMemoryStore_AssignTask(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	task := &proto.TaskInstance{ID: "assign-test", State: proto.TaskStatePending, CreatedAt: time.Now()}
	store.SaveTask(ctx, task)

	if err := store.AssignTask(ctx, "assign-test", "worker-1"); err != nil {
		t.Fatalf("assign: %v", err)
	}

	got, _ := store.GetTask(ctx, "assign-test")
	if got.State != proto.TaskStateRunning {
		t.Fatalf("expected running, got %s", got.State)
	}
	if got.AssignedWorker != "worker-1" {
		t.Fatalf("expected worker-1, got %s", got.AssignedWorker)
	}
}

func TestMemoryStore_ListAndFilter(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		state := proto.TaskStatePending
		if i%2 == 0 {
			state = proto.TaskStateRunning
		}
		store.SaveTask(ctx, &proto.TaskInstance{
			ID:        fmt.Sprintf("list-%d", i),
			State:     state,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	pending, err := store.ListTasks(ctx, storage.TaskFilter{State: proto.TaskStatePending})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pending) != 5 {
		t.Fatalf("expected 5 pending, got %d", len(pending))
	}

	limited, _ := store.ListTasks(ctx, storage.TaskFilter{Limit: 3})
	if len(limited) != 3 {
		t.Fatalf("expected 3 with limit, got %d", len(limited))
	}
}

func TestMemoryStore_CountByState(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	store.SaveTask(ctx, &proto.TaskInstance{ID: "a", State: proto.TaskStatePending, CreatedAt: time.Now()})
	store.SaveTask(ctx, &proto.TaskInstance{ID: "b", State: proto.TaskStatePending, CreatedAt: time.Now()})
	store.SaveTask(ctx, &proto.TaskInstance{ID: "c", State: proto.TaskStateRunning, CreatedAt: time.Now()})

	counts, err := store.CountByState(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counts[proto.TaskStatePending] != 2 {
		t.Fatalf("expected 2 pending, got %d", counts[proto.TaskStatePending])
	}
	if counts[proto.TaskStateRunning] != 1 {
		t.Fatalf("expected 1 running, got %d", counts[proto.TaskStateRunning])
	}
}

func TestMemoryStore_WorkerCRUD(t *testing.T) {
	store := storage.NewMemoryStore(time.Hour)
	ctx := context.Background()

	worker := &proto.WorkerInfo{
		ID:          "w1",
		Address:     "localhost:9100",
		Concurrency: 4,
		Healthy:     true,
	}

	if err := store.SaveWorker(ctx, worker); err != nil {
		t.Fatalf("save worker: %v", err)
	}

	got, err := store.GetWorker(ctx, "w1")
	if err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if got.Concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", got.Concurrency)
	}

	workers, _ := store.ListWorkers(ctx)
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}

	store.DeleteWorker(ctx, "w1")
	_, err = store.GetWorker(ctx, "w1")
	if err == nil {
		t.Fatal("expected error getting deleted worker")
	}
}
