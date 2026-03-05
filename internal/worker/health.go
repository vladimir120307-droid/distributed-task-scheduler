package worker

import (
	"fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

// HealthReporter periodically sends heartbeat signals to the scheduler
// to report the worker liveness and resource utilization.
type HealthReporter struct {
	workerID      string
	schedulerAddr string
	interval      time.Duration
	log           *logger.Logger
	mu            sync.RWMutex
	lastSent      time.Time
	lastAck       time.Time
	failCount     int
	maxFails      int
}

// NewHealthReporter creates a new health reporter.
func NewHealthReporter(workerID, schedulerAddr string, interval time.Duration, log *logger.Logger) *HealthReporter {
	return &HealthReporter{
		workerID:      workerID,
		schedulerAddr: schedulerAddr,
		interval:      interval,
		log:           log,
		maxFails:      5,
	}
}

// Start begins the periodic heartbeat loop. It runs until stopCh is closed.
func (h *HealthReporter) Start(stopCh chan struct{}, wg *sync.WaitGroup, activeTasks *int32) {
	defer wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.log.Info("health reporter started (interval=%s)", h.interval)

	for {
		select {
		case <-stopCh:
			h.log.Info("health reporter stopping")
			return
		case <-ticker.C:
			active := int(atomic.LoadInt32(activeTasks))
			h.sendHeartbeat(active)
		}
	}
}

// sendHeartbeat sends a single heartbeat to the scheduler.
func (h *HealthReporter) sendHeartbeat(activeTasks int) {
	client, err := rpc.DialHTTP("tcp", h.schedulerAddr)
	if err != nil {
		h.recordFailure(fmt.Errorf("dial scheduler: %w", err))
		return
	}
	defer client.Close()

	req := &proto.HeartbeatRequest{
		WorkerID:    h.workerID,
		ActiveTasks: activeTasks,
		CPUUsage:    getCPUUsage(),
		MemUsage:    getMemUsage(),
		Timestamp:   time.Now(),
	}
	var resp proto.HeartbeatResponse

	done := make(chan *rpc.Call, 1)
	client.Go("SchedulerService.Heartbeat", req, &resp, done)

	select {
	case call := <-done:
		if call.Error != nil {
			h.recordFailure(call.Error)
			return
		}
		if resp.Acknowledged {
			h.recordSuccess()
			h.processCommands(resp.Commands)
		}
	case <-time.After(5 * time.Second):
		h.recordFailure(fmt.Errorf("heartbeat RPC timeout"))
	}
}

func (h *HealthReporter) recordSuccess() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSent = time.Now()
	h.lastAck = time.Now()
	h.failCount = 0
}

func (h *HealthReporter) recordFailure(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failCount++
	h.lastSent = time.Now()

	if h.failCount <= 3 {
		h.log.Warn("heartbeat failed (%d/%d): %v", h.failCount, h.maxFails, err)
	} else {
		h.log.Error("heartbeat failed (%d/%d): %v", h.failCount, h.maxFails, err)
	}

	if h.failCount >= h.maxFails {
		h.log.Error("heartbeat failure threshold reached, scheduler may consider this worker dead")
	}
}

func (h *HealthReporter) processCommands(commands []proto.WorkerCommand) {
	for _, cmd := range commands {
		switch cmd.Type {
		case "drain":
			h.log.Info("received drain command from scheduler")
		case "resume":
			h.log.Info("received resume command from scheduler")
		case "shutdown":
			h.log.Warn("received shutdown command from scheduler")
		default:
			h.log.Warn("unknown command type: %s", cmd.Type)
		}
	}
}

// ConsecutiveFailures returns the current failure streak count.
func (h *HealthReporter) ConsecutiveFailures() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.failCount
}

// LastAcknowledged returns the time of the last successful heartbeat.
func (h *HealthReporter) LastAcknowledged() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastAck
}

// IsHealthy returns true if recent heartbeats have been acknowledged.
func (h *HealthReporter) IsHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.failCount < h.maxFails
}

// getCPUUsage returns a simulated CPU usage percentage.
// In production, read from /proc/stat or runtime metrics.
func getCPUUsage() float64 {
	return 0.0
}

// getMemUsage returns a simulated memory usage percentage.
// In production, read from runtime.MemStats or /proc/meminfo.
func getMemUsage() float64 {
	return 0.0
}
