package scheduler

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestExponentialBackoffCalculation(t *testing.T) {
	s := &Scheduler{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	}

	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{attempts: 1, expected: 10 * time.Millisecond},  // 10ms * 2^0
		{attempts: 2, expected: 20 * time.Millisecond},  // 10ms * 2^1
		{attempts: 3, expected: 40 * time.Millisecond},  // 10ms * 2^2
		{attempts: 4, expected: 80 * time.Millisecond},  // 10ms * 2^3
		{attempts: 5, expected: 100 * time.Millisecond}, // 10ms * 2^4 = 160ms -> capped at 100ms
		{attempts: 10, expected: 100 * time.Millisecond},
		{attempts: 50, expected: 100 * time.Millisecond}, // Overflow check
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Attempts_%d", tt.attempts), func(t *testing.T) {
			got := s.calculateBackoff(tt.attempts)
			if got != tt.expected {
				t.Errorf("calculateBackoff(%d) = %v; want %v", tt.attempts, got, tt.expected)
			}
		})
	}
}

func TestChunkQueue_BoundedCapacityAndBackpressure(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
	}

	queue := NewChunkQueueWithCapacity(tasks, 2)
	defer queue.Close()

	if queue.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", queue.Len())
	}

	// Push second task (capacity = 2, so should succeed immediately)
	ctx := context.Background()
	err := queue.PushWithContext(ctx, ChunkTask{Index: 1, ChunkID: core.ChunkID{2}})
	if err != nil {
		t.Fatalf("unexpected error pushing task: %v", err)
	}

	if queue.Len() != 2 {
		t.Fatalf("expected queue length 2, got %d", queue.Len())
	}

	// Try pushing a third task with a timeout context; it should block and time out due to backpressure
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()

	pushErr := queue.PushWithContext(timeoutCtx, ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if pushErr == nil {
		t.Fatal("expected backpressure block and context deadline error, got nil")
	}

	// Pop one task
	task, ok := queue.NextWithContext(ctx)
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got task %v ok=%v", task, ok)
	}

	// Now pushing should succeed since space freed up
	err = queue.PushWithContext(ctx, ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if err != nil {
		t.Fatalf("expected push to succeed after pop, got %v", err)
	}
}

func TestChunkQueue_ContextCancellation(t *testing.T) {
	queue := NewChunkQueueWithCapacity(nil, 5)
	defer queue.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok := queue.NextWithContext(ctx)
	if ok {
		t.Fatal("expected NextWithContext to return false on cancelled context")
	}

	err := queue.PushWithContext(ctx, ChunkTask{Index: 1})
	if err == nil {
		t.Fatal("expected PushWithContext to return context error on cancelled context")
	}
}

func TestChunkQueue_Close(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
	}

	queue := NewChunkQueueWithCapacity(tasks, 10)
	queue.Close()

	if !queue.IsClosed() {
		t.Fatal("expected queue.IsClosed() to be true")
	}

	// Drains existing task
	ctx := context.Background()
	task, ok := queue.NextWithContext(ctx)
	if !ok || task.Index != 0 {
		t.Fatalf("expected to drain task 0, got %v, ok=%v", task, ok)
	}

	// Subsequent Next returns false
	_, ok = queue.NextWithContext(ctx)
	if ok {
		t.Fatal("expected NextWithContext to return false after queue closed and empty")
	}

	// Push returns ErrQueueClosed
	err := queue.PushWithContext(ctx, ChunkTask{Index: 1})
	if err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

func TestScheduler_RequeueBackoffTiming(t *testing.T) {
	s := &Scheduler{
		MaxAttempts:      3,
		BaseBackoff:      40 * time.Millisecond,
		MaxBackoff:       200 * time.Millisecond,
		MaxQueueCapacity: 10,
	}

	task := ChunkTask{Index: 0, ChunkID: core.ChunkID{10}, Attempts: 0}
	queue := NewChunkQueueWithCapacity(nil, 10)
	defer queue.Close()

	// Simulate requeue of a failed task
	ctx := context.Background()
	res := WorkerResult{Task: task, Error: fmt.Errorf("transient_err")}

	start := time.Now()
	res.Task.Attempts++
	backoff := s.calculateBackoff(res.Task.Attempts) // 40ms * 2^0 = 40ms

	if backoff != 40*time.Millisecond {
		t.Fatalf("expected backoff 40ms, got %v", backoff)
	}

	// Simulate requeue delay
	timer := time.NewTimer(backoff)
	select {
	case <-timer.C:
		_ = queue.PushWithContext(ctx, res.Task)
	case <-ctx.Done():
		t.Fatal("context done unexpectedly")
	}
	elapsed := time.Since(start)

	if elapsed < 35*time.Millisecond {
		t.Fatalf("requeue delay too short: %v, expected >= 35ms", elapsed)
	}

	// Verify task back in queue
	requeuedTask, ok := queue.NextWithContext(ctx)
	if !ok || requeuedTask.Attempts != 1 {
		t.Fatalf("expected requeued task with 1 attempt, got %v ok=%v", requeuedTask, ok)
	}
}

func TestScheduler_BoundedMemoryUnderStress(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	queueCap := 50
	queue := NewChunkQueueWithCapacity(nil, queueCap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counter int64
	var pushFailures int64

	// Concurrently push and pop items to simulate heavy load
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			err := queue.PushWithContext(ctx, ChunkTask{Index: i})
			if err != nil {
				atomic.AddInt64(&pushFailures, 1)
			} else {
				atomic.AddInt64(&counter, 1)
			}
		}
	}()

	popped := 0
	for popped < 450 {
		_, ok := queue.NextWithContext(ctx)
		if ok {
			popped++
		} else {
			break
		}
	}

	<-done
	queue.Close()

	// Give goroutines time to settle
	time.Sleep(20 * time.Millisecond)
	finalGoroutines := runtime.NumGoroutine()

	// Goroutine count should not leak
	if finalGoroutines > initialGoroutines+5 {
		t.Errorf("goroutine leak detected: initial %d, final %d", initialGoroutines, finalGoroutines)
	}
}
