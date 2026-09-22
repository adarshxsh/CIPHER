package scheduler

import (
	"testing"

	"cipher/internal/content/core"
)

func TestQueue_CapacityLimit(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{0x01}},
		{Index: 1, ChunkID: core.ChunkID{0x02}},
	}
	q := NewChunkQueueWithCapacity(tasks, 3)

	if q.Cap() != 3 {
		t.Fatalf("expected capacity 3, got %d", q.Cap())
	}
	if q.Len() != 2 || q.Depth() != 2 {
		t.Fatalf("expected length 2, got len=%d depth=%d", q.Len(), q.Depth())
	}

	// Push 3rd item
	if err := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{0x03}}); err != nil {
		t.Fatalf("unexpected error pushing 3rd item: %v", err)
	}

	// Push 4th item (exceeding capacity 3)
	err := q.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{0x04}})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	// Pop one item
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got task=%v ok=%v", task, ok)
	}

	// Now length is 2, pushing should succeed
	if err := q.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{0x04}}); err != nil {
		t.Fatalf("unexpected error pushing after pop: %v", err)
	}
}

func TestQueue_1000SimulatedRetriesMemoryCapped(t *testing.T) {
	initialTask := ChunkTask{Index: 0, ChunkID: core.ChunkID{0xaa}}
	q := NewChunkQueueWithCapacity([]ChunkTask{initialTask}, 10)

	for i := 0; i < 1000; i++ {
		task, ok := q.Next()
		if !ok {
			t.Fatalf("failed to pop task on iteration %d", i)
		}
		task.Attempts++
		if err := q.Push(task); err != nil {
			t.Fatalf("failed to push task on iteration %d: %v", i, err)
		}
		if q.Len() > 1 {
			t.Fatalf("queue depth grew beyond 1: depth=%d", q.Len())
		}
	}

	if q.Cap() != 10 {
		t.Fatalf("expected capacity to remain 10, got %d", q.Cap())
	}
	if q.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", q.Len())
	}
}

func TestQueue_CloseUnblocksWaitingNext(t *testing.T) {
	q := NewChunkQueue(nil)
	done := make(chan bool)

	go func() {
		_, ok := q.Next()
		done <- ok
	}()

	q.Close()
	ok := <-done
	if ok {
		t.Fatalf("expected ok=false after queue close")
	}
}
