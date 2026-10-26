package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_CapacityBounds(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
		{Index: 2, ChunkID: core.ChunkID{3}},
	}

	// Dynamic capacity with maxLimit 5, workerCount 1 (1*2 = 2 < len(tasks), so cap = len(tasks) = 3)
	q := NewChunkQueueWithBounds(tasks, 1, 5)
	if q.Cap() != 3 {
		t.Fatalf("expected cap 3, got %d", q.Cap())
	}
	if q.Len() != 3 {
		t.Fatalf("expected len 3, got %d", q.Len())
	}

	// Test max limit enforcement
	manyTasks := make([]ChunkTask, 20)
	qMax := NewChunkQueueWithBounds(manyTasks, 2, 10)
	if qMax.Cap() != 10 {
		t.Fatalf("expected cap limited to 10, got %d", qMax.Cap())
	}
	if qMax.Len() != 10 {
		t.Fatalf("expected initial queue len 10, got %d", qMax.Len())
	}

	// Attempting non-blocking Push when full should return false and not expand slice
	extraTask := ChunkTask{Index: 99}
	if qMax.Push(extraTask) {
		t.Fatalf("expected Push on full queue to return false")
	}
	if qMax.Len() > qMax.Cap() {
		t.Fatalf("queue length %d exceeded capacity %d", qMax.Len(), qMax.Cap())
	}
}

func TestChunkQueue_PopAndPushCtx(t *testing.T) {
	q := NewChunkQueueWithBounds(nil, 1, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Push task
	taskIn := ChunkTask{Index: 42}
	if !q.PushCtx(ctx, taskIn) {
		t.Fatalf("failed to push task")
	}

	// Pop task
	taskOut, ok := q.Pop(ctx)
	if !ok {
		t.Fatalf("failed to pop task")
	}
	if taskOut.Index != 42 {
		t.Fatalf("expected task index 42, got %d", taskOut.Index)
	}

	// Pop on empty queue with timeout context should return false when context expires
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	_, ok = q.Pop(shortCtx)
	if ok {
		t.Fatalf("expected Pop on empty queue to block and return false on context timeout")
	}
}

func TestChunkQueue_Close(t *testing.T) {
	q := NewChunkQueueWithBounds(nil, 1, 5)
	q.Push(ChunkTask{Index: 1})

	q.Close()
	// Multiple close calls should not panic
	q.Close()

	// Pop should return remaining items first, then ok=false
	t1, ok := q.Pop(context.Background())
	if !ok || t1.Index != 1 {
		t.Fatalf("expected to pop task 1 after close")
	}

	_, ok = q.Pop(context.Background())
	if ok {
		t.Fatalf("expected ok=false on closed empty queue")
	}

	// Push on closed queue should return false
	if q.Push(ChunkTask{Index: 2}) {
		t.Fatalf("expected Push to return false on closed queue")
	}
	if q.PushCtx(context.Background(), ChunkTask{Index: 2}) {
		t.Fatalf("expected PushCtx to return false on closed queue")
	}
}
