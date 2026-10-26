package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_CapacityBounds(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}
	q := NewChunkQueue(tasks, 2)

	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}
	if q.Capacity() != 2 {
		t.Fatalf("expected capacity 2, got %d", q.Capacity())
	}

	// Pushing when full must return ErrQueueOverflow
	err := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if !errors.Is(err, ErrQueueOverflow) {
		t.Fatalf("expected ErrQueueOverflow, got %v", err)
	}

	// Pop one item
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got %v (ok=%v)", task, ok)
	}

	// Now length is 1, so pushing should succeed
	err = q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if err != nil {
		t.Fatalf("expected push success, got %v", err)
	}

	if q.Len() != 2 {
		t.Fatalf("expected length 2 after push, got %d", q.Len())
	}
}

func TestChunkQueue_PopAndClose(t *testing.T) {
	q := NewChunkQueue(nil, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var popped ChunkTask
	var popOk bool

	go func() {
		popped, popOk = q.Pop(ctx)
		close(done)
	}()

	// Ensure Pop is waiting
	time.Sleep(20 * time.Millisecond)

	pushTask := ChunkTask{Index: 42, ChunkID: core.ChunkID{42}}
	if err := q.Push(pushTask); err != nil {
		t.Fatalf("failed to push task: %v", err)
	}

	<-done
	if !popOk || popped.Index != 42 {
		t.Fatalf("expected popped task index 42, got %v (ok=%v)", popped, popOk)
	}

	// Test Close unblocks waiting Pop
	doneClose := make(chan struct{})
	go func() {
		_, ok := q.Pop(ctx)
		if ok {
			t.Errorf("expected Pop on closed queue to return false")
		}
		close(doneClose)
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()
	<-doneClose

	// Pushing to closed queue should fail
	if err := q.Push(pushTask); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}
