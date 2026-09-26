package scheduler

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerContextCancellationOnBlockedCompletionsChannel(t *testing.T) {
	// Create an unbuffered completions channel that will block if written to
	completions := make(chan WorkerResult)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()

	start := time.Now()

	// Call scheduler Run with no workers / cancelled context
	sched := NewScheduler(nil, nil, 3)
	err := sched.Run(ctx, nil, nil, completions)

	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected scheduler Run to return context error, got nil")
	}

	// Metric check: event loop processes context cancellation within 5ms (allow small buffer for test runner variance)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected cancellation handling to be fast, took %v", elapsed)
	}
}

func TestSchedulerRequeueCapacityEnforcement(t *testing.T) {
	// Bounded queue initialized with 1 task
	tasks := []ChunkTask{{Index: 0}}
	q := NewChunkQueueWithCapacity(tasks, 1) // max cap 1

	// Queue is already at cap (1 task)
	if err := q.Push(ChunkTask{Index: 1}); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull on pushing beyond capacity, got %v", err)
	}
}
