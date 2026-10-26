package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_Basic(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
		{Index: 2, ChunkID: core.ChunkID{3}},
	}

	q := NewChunkQueue(tasks)
	if q.Len() != 3 {
		t.Fatalf("expected Len 3, got %d", q.Len())
	}

	task1, ok := q.Next()
	if !ok || task1.Index != 0 {
		t.Fatalf("expected task 0, got %v, ok=%v", task1, ok)
	}

	if q.Len() != 2 {
		t.Fatalf("expected Len 2, got %d", q.Len())
	}

	// Push task1 back
	q.Push(task1)
	if q.Len() != 3 {
		t.Fatalf("expected Len 3 after Push, got %d", q.Len())
	}

	task2, ok := q.Next()
	if !ok || task2.Index != 1 {
		t.Fatalf("expected task 1, got %v", task2)
	}

	task3, ok := q.Next()
	if !ok || task3.Index != 2 {
		t.Fatalf("expected task 2, got %v", task3)
	}

	task1Again, ok := q.Next()
	if !ok || task1Again.Index != 0 {
		t.Fatalf("expected task 0 again, got %v", task1Again)
	}

	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got Len %d", q.Len())
	}
}

func TestChunkQueue_RingBufferWrap(t *testing.T) {
	// Create queue with 1 task
	q := NewChunkQueue([]ChunkTask{{Index: 100}})

	// Repeatedly pop and push 1000 times to test wrap-around without allocation creep
	for i := 0; i < 1000; i++ {
		task, ok := q.Next()
		if !ok || task.Index != 100 {
			t.Fatalf("iteration %d: expected task 100, got %v, ok=%v", i, task, ok)
		}
		q.Push(task)
	}

	if q.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", q.Len())
	}
}

func TestChunkQueue_BackoffDelay(t *testing.T) {
	q := NewChunkQueue([]ChunkTask{})

	delayedTask := ChunkTask{
		Index:         5,
		NextAvailable: time.Now().Add(100 * time.Millisecond),
	}
	readyTask := ChunkTask{
		Index:         6,
		NextAvailable: time.Time{},
	}

	q.Push(delayedTask)
	q.Push(readyTask)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Next should skip delayedTask and pop readyTask first!
	task, ok := q.NextWithContext(ctx)
	if !ok || task.Index != 6 {
		t.Fatalf("expected ready task 6 to be popped first, got task index %d, ok=%v", task.Index, ok)
	}

	// Next call for delayed task should wait until its NextAvailable time passes
	start := time.Now()
	taskDelayed, ok := q.NextWithContext(ctx)
	elapsed := time.Since(start)

	if !ok || taskDelayed.Index != 5 {
		t.Fatalf("expected delayed task 5, got %v", taskDelayed)
	}

	if elapsed < 30*time.Millisecond {
		t.Fatalf("expected Next to wait for delayed task backoff, but returned in %v", elapsed)
	}
}

func TestChunkQueue_Close(t *testing.T) {
	q := NewChunkQueue([]ChunkTask{})

	doneCh := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, ok := q.NextWithContext(ctx)
		if ok {
			t.Errorf("expected ok=false on closed queue")
		}
		close(doneCh)
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case <-doneCh:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for worker to wake up on queue close")
	}
}

func TestCalculateBackoff(t *testing.T) {
	base := 50 * time.Millisecond

	if b := CalculateBackoff(0, base); b != 0 {
		t.Errorf("expected 0 for attempt 0, got %v", b)
	}
	if b := CalculateBackoff(1, base); b != 50*time.Millisecond {
		t.Errorf("expected 50ms for attempt 1, got %v", b)
	}
	if b := CalculateBackoff(2, base); b != 100*time.Millisecond {
		t.Errorf("expected 100ms for attempt 2, got %v", b)
	}
	if b := CalculateBackoff(3, base); b != 200*time.Millisecond {
		t.Errorf("expected 200ms for attempt 3, got %v", b)
	}
}

func TestChunkQueue_ZeroAllocations(t *testing.T) {
	tasks := make([]ChunkTask, 100)
	for i := 0; i < 100; i++ {
		tasks[i] = ChunkTask{Index: i}
	}
	q := NewChunkQueue(tasks)

	allocs := testing.AllocsPerRun(1000, func() {
		task, ok := q.Next()
		if !ok {
			t.Fatalf("failed to pop task")
		}
		q.Push(task)
	})

	if allocs > 0 {
		t.Fatalf("expected 0 heap allocations during queue Push/Next, got %f allocs/op", allocs)
	}
}
