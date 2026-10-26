package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_BoundedCapacity(t *testing.T) {
	q := NewChunkQueue(3)

	if q.Cap() != 3 {
		t.Fatalf("expected capacity 3, got %d", q.Cap())
	}

	task1 := ChunkTask{Index: 0, ChunkID: core.ChunkID{1}}
	task2 := ChunkTask{Index: 1, ChunkID: core.ChunkID{2}}
	task3 := ChunkTask{Index: 2, ChunkID: core.ChunkID{3}}
	task4 := ChunkTask{Index: 3, ChunkID: core.ChunkID{4}}

	if err := q.Push(task1); err != nil {
		t.Fatalf("failed to push task1: %v", err)
	}
	if err := q.Push(task2); err != nil {
		t.Fatalf("failed to push task2: %v", err)
	}
	if err := q.Push(task3); err != nil {
		t.Fatalf("failed to push task3: %v", err)
	}

	if err := q.Push(task4); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull when pushing over capacity, got: %v", err)
	}

	if q.Len() != 3 {
		t.Fatalf("expected Len 3, got %d", q.Len())
	}
}

func TestChunkQueue_FIFOAndWrapAround(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
		{Index: 2, ChunkID: core.ChunkID{3}},
	}

	q := NewChunkQueue(tasks)

	if q.Cap() != 3 {
		t.Fatalf("expected capacity 3, got %d", q.Cap())
	}

	// Pop 1
	item, ok := q.Next()
	if !ok || item.Index != 0 {
		t.Fatalf("expected index 0, got %v (ok=%v)", item, ok)
	}

	// Push 1 back (causes circular wrap-around)
	newTask := ChunkTask{Index: 3, ChunkID: core.ChunkID{4}}
	if err := q.Push(newTask); err != nil {
		t.Fatalf("failed to push: %v", err)
	}

	// Pop remaining
	expectedIndices := []int{1, 2, 3}
	for _, expectedIdx := range expectedIndices {
		task, ok := q.Next()
		if !ok || task.Index != expectedIdx {
			t.Fatalf("expected index %d, got %v (ok=%v)", expectedIdx, task, ok)
		}
	}

	// Queue should now be empty
	_, ok = q.Next()
	if ok {
		t.Fatalf("expected queue to be empty")
	}
}

func TestChunkQueue_PushWithBackoff(t *testing.T) {
	q := NewChunkQueue(1)

	task := ChunkTask{Index: 0, ChunkID: core.ChunkID{1}, Attempts: 1}
	delay := 100 * time.Millisecond
	start := time.Now()

	if err := q.PushWithBackoff(task, delay); err != nil {
		t.Fatalf("failed to push with backoff: %v", err)
	}

	if q.ActiveLen() != 0 {
		t.Fatalf("expected 0 active tasks immediately, got %d", q.ActiveLen())
	}
	if q.DelayedLen() != 1 {
		t.Fatalf("expected 1 delayed task, got %d", q.DelayedLen())
	}

	// Next() should wait until delay elapses
	gotTask, ok := q.Next()
	elapsed := time.Since(start)

	if !ok {
		t.Fatalf("expected to receive task after backoff delay")
	}
	if gotTask.Index != 0 {
		t.Fatalf("expected task index 0, got %d", gotTask.Index)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected elapsed time to be at least ~100ms, got %v", elapsed)
	}
}

func TestChunkQueue_ContextCancellation(t *testing.T) {
	q := NewChunkQueue(1)

	task := ChunkTask{Index: 0, ChunkID: core.ChunkID{1}}
	_ = q.PushWithBackoff(task, 5*time.Second) // Long delay

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, ok := q.NextWithContext(ctx)
	elapsed := time.Since(start)

	if ok {
		t.Fatalf("expected NextWithContext to return false on context cancellation")
	}
	if elapsed >= 1*time.Second {
		t.Fatalf("expected prompt return on context cancellation, took %v", elapsed)
	}
}
