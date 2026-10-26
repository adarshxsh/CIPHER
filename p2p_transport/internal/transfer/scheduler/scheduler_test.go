package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueueCapacity(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}
	// Capacity explicit limit = 2
	q := NewChunkQueue(initialTasks, 2)
	if q.Capacity() != 2 {
		t.Fatalf("expected capacity 2, got %d", q.Capacity())
	}
	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}

	// Try pushing to full queue
	pushed := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if pushed {
		t.Fatalf("expected Push to return false on full queue")
	}

	// Pop one item
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got %v, ok=%v", task, ok)
	}

	if q.Len() != 1 {
		t.Fatalf("expected length 1, got %d", q.Len())
	}

	// Now Push should succeed
	pushed = q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if !pushed {
		t.Fatalf("expected Push to return true when space available")
	}
}

func TestChunkQueueBlockingAndClose(t *testing.T) {
	q := NewChunkQueue([]ChunkTask{}, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var poppedTask ChunkTask
	var poppedOk bool

	go func() {
		poppedTask, poppedOk = q.NextWithContext(ctx)
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("NextWithContext should have blocked on empty queue")
	case <-time.After(50 * time.Millisecond):
		// Expected to block
	}

	// Push task
	pushedTask := ChunkTask{Index: 42, ChunkID: core.ChunkID{99}}
	if !q.Push(pushedTask) {
		t.Fatalf("failed to push task")
	}

	select {
	case <-done:
		if !poppedOk || poppedTask.Index != 42 {
			t.Fatalf("expected task 42, got %v, ok=%v", poppedTask, poppedOk)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("NextWithContext failed to unblock after Push")
	}

	// Test Close unblocks waiting NextWithContext
	doneClose := make(chan struct{})
	go func() {
		_, ok := q.NextWithContext(ctx)
		if ok {
			t.Errorf("expected NextWithContext to return ok=false on closed queue")
		}
		close(doneClose)
	}()

	select {
	case <-doneClose:
		t.Fatalf("NextWithContext should block on empty queue")
	case <-time.After(50 * time.Millisecond):
	}

	q.Close()

	select {
	case <-doneClose:
		// Unblocked successfully
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("NextWithContext failed to unblock after queue.Close()")
	}
}

func TestCalculateBackoff(t *testing.T) {
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	// Attempt 1: base ~ 100ms, with jitter [100ms, 200ms)
	b1 := calculateBackoff(1, baseBackoff, maxBackoff)
	if b1 < 100*time.Millisecond || b1 > 200*time.Millisecond {
		t.Fatalf("attempt 1 backoff out of bounds: %v", b1)
	}

	// Attempt 2: base ~ 200ms, with jitter [200ms, 300ms)
	b2 := calculateBackoff(2, baseBackoff, maxBackoff)
	if b2 < 200*time.Millisecond || b2 > 300*time.Millisecond {
		t.Fatalf("attempt 2 backoff out of bounds: %v", b2)
	}

	// Attempt 10: should cap at maxBackoff (1s)
	b10 := calculateBackoff(10, baseBackoff, maxBackoff)
	if b10 > maxBackoff {
		t.Fatalf("attempt 10 backoff exceeded maxBackoff: %v", b10)
	}
}

func TestSchedulerAsyncCompletionsNoBlock(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	completions := make(chan WorkerResult)
	asyncCompletions := make(chan WorkerResult, 100)
	done := make(chan struct{})

	go func() {
		for _, task := range tasks {
			asyncCompletions <- WorkerResult{Task: task}
		}
		close(asyncCompletions)
		close(done)
	}()

	select {
	case <-done:
		// Successfully sent all results without blocking on completions channel receiver
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("async completions blocked when sending to async buffer")
	}

	// Read completions with a slow receiver
	count := 0
	go func() {
		for res := range asyncCompletions {
			completions <- res
		}
		close(completions)
	}()

	for range completions {
		count++
		time.Sleep(10 * time.Millisecond) // Slow receiver simulation
	}

	if count != 2 {
		t.Fatalf("expected 2 completions, got %d", count)
	}
}

func TestSchedulerContextCancellation(t *testing.T) {
	q := NewChunkQueue([]ChunkTask{{Index: 0}}, 10)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context immediately
	cancel()

	_, ok := q.NextWithContext(ctx)
	if ok {
		t.Fatalf("expected NextWithContext to return ok=false on canceled context")
	}
}

func TestQueueDefaultCapacity(t *testing.T) {
	tasks := make([]ChunkTask, 5)
	q := NewChunkQueue(tasks)
	if q.Capacity() != 10 { // 5 * 2
		t.Fatalf("expected default capacity 10 for 5 tasks, got %d", q.Capacity())
	}

	qEmpty := NewChunkQueue(nil)
	if qEmpty.Capacity() != 100 { // fallback default
		t.Fatalf("expected default capacity 100 for empty tasks, got %d", qEmpty.Capacity())
	}
}
