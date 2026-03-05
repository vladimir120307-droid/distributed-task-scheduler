package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Policy defines the parameters for exponential backoff with jitter.
type Policy struct {
	MaxRetries     int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	JitterFraction float64
}

// DefaultPolicy returns a production-ready retry policy.
func DefaultPolicy() *Policy {
	return &Policy{
		MaxRetries:     3,
		BaseDelay:      1 * time.Second,
		MaxDelay:       60 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
}

// RetryableError marks an error as eligible for retry.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// Retryable wraps an error to indicate it can be retried.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// IsRetryable returns true if the error was wrapped with Retryable.
func IsRetryable(err error) bool {
	_, ok := err.(*RetryableError)
	return ok
}

// Attempt holds the result details of a single execution attempt.
type Attempt struct {
	Number   int
	Err      error
	Duration time.Duration
}

// Result aggregates the outcome of all retry attempts.
type Result struct {
	Attempts []Attempt
	Err      error
	Success  bool
}

// Do executes fn up to p.MaxRetries+1 times, backing off between failures.
// It respects context cancellation. Only errors wrapped with Retryable trigger
// further attempts; other errors cause immediate return.
func (p *Policy) Do(ctx context.Context, fn func(ctx context.Context) error) *Result {
	result := &Result{
		Attempts: make([]Attempt, 0, p.MaxRetries+1),
	}

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			return result
		}

		start := time.Now()
		err := fn(ctx)
		elapsed := time.Since(start)

		result.Attempts = append(result.Attempts, Attempt{
			Number:   attempt + 1,
			Err:      err,
			Duration: elapsed,
		})

		if err == nil {
			result.Success = true
			return result
		}

		if !IsRetryable(err) {
			result.Err = err
			return result
		}

		if attempt == p.MaxRetries {
			result.Err = fmt.Errorf("max retries (%d) exceeded: %w", p.MaxRetries, err)
			return result
		}

		delay := p.calculateDelay(attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Err = ctx.Err()
			return result
		case <-timer.C:
		}
	}

	return result
}

// calculateDelay computes the backoff duration for a given attempt number,
// applying exponential growth capped at MaxDelay, with random jitter.
func (p *Policy) calculateDelay(attempt int) time.Duration {
	delay := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt))
	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	if p.JitterFraction > 0 {
		jitter := delay * p.JitterFraction
		delay = delay - jitter + (rand.Float64() * 2 * jitter)
	}

	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

// DoSimple is a convenience wrapper around Do that returns only the final error.
func (p *Policy) DoSimple(ctx context.Context, fn func(ctx context.Context) error) error {
	result := p.Do(ctx, fn)
	return result.Err
}

// WithMaxRetries returns a copy of the policy with a different retry limit.
func (p *Policy) WithMaxRetries(n int) *Policy {
	cp := *p
	cp.MaxRetries = n
	return &cp
}

// WithBaseDelay returns a copy of the policy with a different initial delay.
func (p *Policy) WithBaseDelay(d time.Duration) *Policy {
	cp := *p
	cp.BaseDelay = d
	return &cp
}

// WithMaxDelay returns a copy of the policy with a different delay cap.
func (p *Policy) WithMaxDelay(d time.Duration) *Policy {
	cp := *p
	cp.MaxDelay = d
	return &cp
}

// WithMultiplier returns a copy of the policy with a different growth factor.
func (p *Policy) WithMultiplier(m float64) *Policy {
	cp := *p
	cp.Multiplier = m
	return &cp
}

// WithJitter returns a copy of the policy with a different jitter fraction.
func (p *Policy) WithJitter(j float64) *Policy {
	cp := *p
	cp.JitterFraction = j
	return &cp
}
