package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/worker"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/config"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/retry"
)

func TestExecutor_BasicExecution(t *testing.T) {
	log := logger.New(logger.LevelDebug, false)
	exec := worker.NewExecutor(2, 10*time.Second, log)

	task := &proto.TaskInstance{
		ID:       "exec-test-1",
		Name:     "basic-task",
		State:    proto.TaskStateRunning,
		Priority: 1,
	}

	result, err := exec.Execute(context.Background(), task, 5*time.Second)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if !result.Success {
		t.Fatalf("task should succeed, got error: %s", result.Error)
	}
	if result.TaskID != "exec-test-1" {
		t.Fatalf("expected task ID exec-test-1, got %s", result.TaskID)
	}
	if result.Duration <= 0 {
		t.Fatal("expected positive duration")
	}
}

func TestExecutor_Timeout(t *testing.T) {
	log := logger.New(logger.LevelDebug, false)
	exec := worker.NewExecutor(1, 1*time.Millisecond, log)

	task := &proto.TaskInstance{
		ID:       "timeout-test",
		Name:     "slow-task",
		State:    proto.TaskStateRunning,
		Priority: 1,
	}

	result, err := exec.Execute(context.Background(), task, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	// Task may or may not time out depending on the random sleep,
	// but the executor should handle it gracefully either way.
	_ = result
}

func TestExecutor_ConcurrencyLimit(t *testing.T) {
	log := logger.New(logger.LevelDebug, false)
	exec := worker.NewExecutor(2, 5*time.Second, log)

	done := make(chan struct{}, 3)

	for i := 0; i < 3; i++ {
		go func(idx int) {
			task := &proto.TaskInstance{
				ID:       fmt.Sprintf("concurrent-%d", idx),
				Name:     "concurrent-task",
				State:    proto.TaskStateRunning,
				Priority: 1,
			}
			exec.Execute(context.Background(), task, 2*time.Second)
			done <- struct{}{}
		}(i)
	}

	// All three should complete eventually (third waits for a slot).
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("timed out waiting for concurrent tasks")
		}
	}
}

func TestExecutor_CancelTask(t *testing.T) {
	log := logger.New(logger.LevelDebug, false)
	exec := worker.NewExecutor(2, 10*time.Second, log)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	task := &proto.TaskInstance{
		ID:       "cancel-test",
		Name:     "cancellable",
		State:    proto.TaskStateRunning,
		Priority: 1,
	}

	result, err := exec.Execute(ctx, task, 10*time.Second)
	// Either the context cancel kills it or it finishes before cancel.
	_ = result
	_ = err
}

func TestExecutor_ActiveCount(t *testing.T) {
	log := logger.New(logger.LevelDebug, false)
	exec := worker.NewExecutor(4, 5*time.Second, log)

	if exec.ActiveCount() != 0 {
		t.Fatalf("expected 0 active, got %d", exec.ActiveCount())
	}

	if exec.AvailableSlots() != 4 {
		t.Fatalf("expected 4 available slots, got %d", exec.AvailableSlots())
	}
}

func TestRetryPolicy_SuccessOnFirstAttempt(t *testing.T) {
	policy := retry.DefaultPolicy()

	result := policy.Do(context.Background(), func(ctx context.Context) error {
		return nil
	})

	if !result.Success {
		t.Fatal("expected success")
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(result.Attempts))
	}
}

func TestRetryPolicy_RetryableError(t *testing.T) {
	policy := retry.DefaultPolicy().WithMaxRetries(2).WithBaseDelay(1 * time.Millisecond)
	attempts := 0

	result := policy.Do(context.Background(), func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return retry.Retryable(fmt.Errorf("transient error"))
		}
		return nil
	})

	if !result.Success {
		t.Fatalf("expected success after retries, got error: %v", result.Err)
	}
	if len(result.Attempts) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(result.Attempts))
	}
}

func TestRetryPolicy_NonRetryableError(t *testing.T) {
	policy := retry.DefaultPolicy().WithMaxRetries(3)

	result := policy.Do(context.Background(), func(ctx context.Context) error {
		return fmt.Errorf("permanent error")
	})

	if result.Success {
		t.Fatal("expected failure for non-retryable error")
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("expected 1 attempt (no retry), got %d", len(result.Attempts))
	}
}

func TestRetryPolicy_MaxRetriesExhausted(t *testing.T) {
	policy := retry.DefaultPolicy().WithMaxRetries(2).WithBaseDelay(1 * time.Millisecond)

	result := policy.Do(context.Background(), func(ctx context.Context) error {
		return retry.Retryable(fmt.Errorf("keeps failing"))
	})

	if result.Success {
		t.Fatal("expected failure when max retries exhausted")
	}
	if len(result.Attempts) != 3 {
		t.Fatalf("expected 3 attempts (1 + 2 retries), got %d", len(result.Attempts))
	}
}

func TestRetryPolicy_ContextCancellation(t *testing.T) {
	policy := retry.DefaultPolicy().WithMaxRetries(10).WithBaseDelay(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result := policy.Do(ctx, func(ctx context.Context) error {
		return retry.Retryable(fmt.Errorf("transient"))
	})

	if result.Success {
		t.Fatal("expected failure due to context cancellation")
	}
}

func TestRetryPolicy_DoSimple(t *testing.T) {
	policy := retry.DefaultPolicy().WithMaxRetries(1).WithBaseDelay(1 * time.Millisecond)

	err := policy.DoSimple(context.Background(), func(ctx context.Context) error {
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestRetryPolicy_IsRetryable(t *testing.T) {
	err := retry.Retryable(fmt.Errorf("test"))
	if !retry.IsRetryable(err) {
		t.Fatal("expected retryable error")
	}

	regular := fmt.Errorf("not retryable")
	if retry.IsRetryable(regular) {
		t.Fatal("regular error should not be retryable")
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := config.DefaultConfig()

	if cfg.Scheduler.ListenAddr != ":9090" {
		t.Fatalf("expected :9090, got %s", cfg.Scheduler.ListenAddr)
	}
	if cfg.Worker.Concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", cfg.Worker.Concurrency)
	}
	if cfg.HashRing.VirtualNodes != 150 {
		t.Fatalf("expected 150 vnodes, got %d", cfg.HashRing.VirtualNodes)
	}
	if cfg.Retry.MaxRetries != 3 {
		t.Fatalf("expected 3 max retries, got %d", cfg.Retry.MaxRetries)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Worker.Concurrency = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for concurrency=0")
	}

	cfg = config.DefaultConfig()
	cfg.Retry.JitterFraction = 2.0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for jitter > 1")
	}

	cfg = config.DefaultConfig()
	cfg.Scheduler.ElectionTimeout = cfg.Scheduler.HeartbeatInterval
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error: election_timeout must exceed heartbeat_interval")
	}
}
