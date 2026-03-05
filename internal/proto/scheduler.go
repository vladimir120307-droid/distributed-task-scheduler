package proto

import (
	"fmt"
	"time"
)

// TaskState represents the lifecycle state of a task.
type TaskState string

const (
	TaskStatePending   TaskState = "pending"
	TaskStateRunning   TaskState = "running"
	TaskStateCompleted TaskState = "completed"
	TaskStateFailed    TaskState = "failed"
	TaskStateRetrying  TaskState = "retrying"
	TaskStateCancelled TaskState = "cancelled"
)

// ValidTransitions defines the allowed state machine transitions for tasks.
var ValidTransitions = map[TaskState][]TaskState{
	TaskStatePending:   {TaskStateRunning, TaskStateCancelled},
	TaskStateRunning:   {TaskStateCompleted, TaskStateFailed, TaskStateCancelled},
	TaskStateFailed:    {TaskStateRetrying, TaskStateCancelled},
	TaskStateRetrying:  {TaskStateRunning, TaskStateCancelled},
	TaskStateCompleted: {},
	TaskStateCancelled: {},
}

// CanTransition checks whether a state transition is allowed.
func CanTransition(from, to TaskState) bool {
	allowed, ok := ValidTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// TaskDefinition is the user-submitted task specification.
type TaskDefinition struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Payload      []byte   `json:"payload"`
	Priority     int      `json:"priority"`
	CronExpr     string   `json:"cron_expr,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
	MaxRetries   int      `json:"max_retries"`
	Timeout      string   `json:"timeout,omitempty"`
}

// TaskInstance is the runtime representation of a scheduled task.
type TaskInstance struct {
	ID             string    `json:"id"`
	DefinitionID   string    `json:"definition_id"`
	Name           string    `json:"name"`
	Payload        []byte    `json:"payload"`
	Priority       int       `json:"priority"`
	State          TaskState `json:"state"`
	AssignedWorker string    `json:"assigned_worker,omitempty"`
	RetryCount     int       `json:"retry_count"`
	MaxRetries     int       `json:"max_retries"`
	CreatedAt      time.Time `json:"created_at"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	Error          string    `json:"error,omitempty"`
	Result         []byte    `json:"result,omitempty"`
	Dependencies   []string  `json:"dependencies,omitempty"`
	CronExpr       string    `json:"cron_expr,omitempty"`
}

// TransitionTo attempts a state transition and returns an error if the transition
// violates the state machine rules.
func (t *TaskInstance) TransitionTo(next TaskState) error {
	if !CanTransition(t.State, next) {
		return fmt.Errorf("invalid transition from %s to %s for task %s", t.State, next, t.ID)
	}
	t.State = next
	switch next {
	case TaskStateRunning:
		t.StartedAt = time.Now()
	case TaskStateCompleted, TaskStateFailed, TaskStateCancelled:
		t.CompletedAt = time.Now()
	}
	return nil
}

// SubmitTaskRequest is the RPC request for submitting a single task.
type SubmitTaskRequest struct {
	Task *TaskDefinition
}

// SubmitTaskResponse is the RPC response after task submission.
type SubmitTaskResponse struct {
	TaskID string
	Status string
}

// SubmitDAGRequest is the RPC request for submitting a directed acyclic graph of tasks.
type SubmitDAGRequest struct {
	Tasks []*TaskDefinition
}

// SubmitDAGResponse is the RPC response after DAG submission.
type SubmitDAGResponse struct {
	TaskIDs []string
	Status  string
}

// TaskStatusRequest is the RPC request for querying task state.
type TaskStatusRequest struct {
	TaskID string
}

// TaskStatusResponse is the RPC response with task state details.
type TaskStatusResponse struct {
	TaskID         string
	State          TaskState
	AssignedWorker string
	RetryCount     int
	Error          string
	CreatedAt      time.Time
	StartedAt      time.Time
	CompletedAt    time.Time
}

// CancelTaskRequest is the RPC request to cancel a task.
type CancelTaskRequest struct {
	TaskID string
}

// CancelTaskResponse is the RPC response after cancellation.
type CancelTaskResponse struct {
	Success bool
	Message string
}

// WorkerInfo describes a worker node in the cluster.
type WorkerInfo struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	Concurrency   int       `json:"concurrency"`
	ActiveTasks   int       `json:"active_tasks"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Healthy       bool      `json:"healthy"`
	Tags          []string  `json:"tags,omitempty"`
}

// RegisterWorkerRequest is the RPC request for worker registration.
type RegisterWorkerRequest struct {
	WorkerID    string
	Address     string
	Concurrency int
	Tags        []string
}

// RegisterWorkerResponse is the RPC response after registration.
type RegisterWorkerResponse struct {
	Accepted     bool
	LeaderAddr   string
	Message      string
	AssignedSlot int
}

// HeartbeatRequest is the periodic liveness signal from worker to scheduler.
type HeartbeatRequest struct {
	WorkerID    string
	ActiveTasks int
	CPUUsage    float64
	MemUsage    float64
	Timestamp   time.Time
}

// HeartbeatResponse is the scheduler's reply to a heartbeat.
type HeartbeatResponse struct {
	Acknowledged bool
	Commands     []WorkerCommand
}

// WorkerCommand is an instruction sent from scheduler to worker.
type WorkerCommand struct {
	Type    string // "drain", "resume", "shutdown"
	Payload []byte
}

// ExecuteTaskRequest is the RPC request sent to a worker to run a task.
type ExecuteTaskRequest struct {
	Task    *TaskInstance
	Timeout time.Duration
}

// ExecuteTaskResponse is the worker's reply after task execution.
type ExecuteTaskResponse struct {
	TaskID    string
	Success   bool
	Result    []byte
	Error     string
	Duration  time.Duration
	ExitCode  int
}

// LeaderElectionMessage is exchanged between scheduler nodes during elections.
type LeaderElectionMessage struct {
	NodeID    string
	Term      uint64
	Timestamp time.Time
	IsLeader  bool
}

// LeaderElectionResponse is the reply to an election message.
type LeaderElectionResponse struct {
	Accepted   bool
	LeaderID   string
	LeaderAddr string
	Term       uint64
}

// ListWorkersRequest is the RPC request to list cluster workers.
type ListWorkersRequest struct{}

// ListWorkersResponse is the RPC response with the full worker roster.
type ListWorkersResponse struct {
	Workers []*WorkerInfo
}

// ClusterStatusRequest is the RPC request for cluster-wide status.
type ClusterStatusRequest struct{}

// ClusterStatusResponse returns high-level cluster metrics.
type ClusterStatusResponse struct {
	LeaderID       string
	TotalWorkers   int
	HealthyWorkers int
	PendingTasks   int
	RunningTasks   int
	CompletedTasks int
	FailedTasks    int
}
