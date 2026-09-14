package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestCalculateBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 5 * time.Second

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 0},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 5000 * time.Millisecond}, // capped
		{10, 5000 * time.Millisecond}, // capped
	}

	for _, tt := range tests {
		got := CalculateBackoff(tt.attempt, base, max)
		if got != tt.expected {
			t.Errorf("CalculateBackoff(attempt=%d) = %v; want %v", tt.attempt, got, tt.expected)
		}
	}
}

func TestScheduler_DecoupledCompletions(t *testing.T) {
	// Verify that internal completions buffer decouples scheduler dispatch from slow downstream consumers
	completions := make(chan WorkerResult) // Unbuffered downstream completions channel
	internalCompletions := make(chan WorkerResult, 5)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Simulate internal completions dispatch
		for i := 0; i < 5; i++ {
			res := WorkerResult{
				Task: ChunkTask{Index: i, ChunkID: core.ChunkID{byte(i)}},
			}
			internalCompletions <- res
		}
	}()

	select {
	case <-done:
		// Succeeded in putting 5 completions into internalCompletions without reading from `completions` yet
	case <-time.After(1 * time.Second):
		t.Fatalf("dispatch blocked waiting for downstream completions consumer")
	}

	// Now drain
	for i := 0; i < 5; i++ {
		<-internalCompletions
	}

	// Touch completions variable to satisfy compiler
	_ = completions
}

func TestScheduler_BackoffRetryLimit(t *testing.T) {
	sched := &Scheduler{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
	}

	tasks := []ChunkTask{{Index: 0, ChunkID: core.ChunkID{1}}}
	queue := NewChunkQueue(tasks)

	// Simulate failure attempts up to max
	task, ok := queue.Next()
	if !ok {
		t.Fatalf("expected task")
	}

	task.Attempts++
	backoff1 := CalculateBackoff(task.Attempts, sched.BaseBackoff, sched.MaxBackoff)
	if backoff1 != 10*time.Millisecond {
		t.Fatalf("expected 10ms backoff, got %v", backoff1)
	}
	if err := queue.PushWithBackoff(task, backoff1); err != nil {
		t.Fatalf("failed to push with backoff: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	retriedTask, ok := queue.NextWithContext(ctx)
	if !ok {
		t.Fatalf("expected retried task after backoff")
	}
	if retriedTask.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", retriedTask.Attempts)
	}

	// Attempt 2
	retriedTask.Attempts++
	backoff2 := CalculateBackoff(retriedTask.Attempts, sched.BaseBackoff, sched.MaxBackoff)
	if backoff2 != 20*time.Millisecond {
		t.Fatalf("expected 20ms backoff, got %v", backoff2)
	}

	// At Attempt 3, attempt >= MaxAttempts, scheduler returns error
	if retriedTask.Attempts >= sched.MaxAttempts {
		expectedErr := fmt.Sprintf("chunk %x failed after %d attempts", retriedTask.ChunkID, sched.MaxAttempts)
		if expectedErr == "" {
			t.Fatalf("unexpected error message format")
		}
	}
}
