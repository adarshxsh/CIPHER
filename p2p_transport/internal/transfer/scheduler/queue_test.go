package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestQueueCapacityAndRingBuffer(t *testing.T) {
	cap := 5
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}
	q := NewChunkQueueWithCapacity(initialTasks, cap)

	if q.Len() != 2 {
		t.Fatalf("expected len 2, got %d", q.Len())
	}
	if q.Cap() != cap {
		t.Fatalf("expected cap %d, got %d", cap, q.Cap())
	}

	// Fill queue up to capacity (3 more tasks)
	for i := 2; i < 5; i++ {
		err := q.Push(ChunkTask{Index: i, ChunkID: core.ChunkID{byte(i + 1)}})
		if err != nil {
			t.Fatalf("unexpected error pushing task %d: %v", i, err)
		}
	}

	if q.Len() != 5 {
		t.Fatalf("expected len 5, got %d", q.Len())
	}

	// Exceed capacity
	err := q.Push(ChunkTask{Index: 5, ChunkID: core.ChunkID{6}})
	if err == nil || err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull when pushing to full queue, got: %v", err)
	}

	// Pop 2 tasks
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task 0, got %v, ok=%v", task, ok)
	}
	task, ok = q.Next()
	if !ok || task.Index != 1 {
		t.Fatalf("expected task 1, got %v, ok=%v", task, ok)
	}

	if q.Len() != 3 {
		t.Fatalf("expected len 3, got %d", q.Len())
	}

	// Push 2 more tasks to test ring buffer wraparound
	err = q.Push(ChunkTask{Index: 5, ChunkID: core.ChunkID{6}})
	if err != nil {
		t.Fatalf("failed pushing task 5: %v", err)
	}
	err = q.Push(ChunkTask{Index: 6, ChunkID: core.ChunkID{7}})
	if err != nil {
		t.Fatalf("failed pushing task 6: %v", err)
	}

	if q.Len() != 5 {
		t.Fatalf("expected len 5, got %d", q.Len())
	}

	// Verify order
	expectedIndices := []int{2, 3, 4, 5, 6}
	for _, expectedIdx := range expectedIndices {
		tSK, ok := q.Next()
		if !ok || tSK.Index != expectedIdx {
			t.Fatalf("expected index %d, got %d (ok=%v)", expectedIdx, tSK.Index, ok)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("expected len 0 after popping all, got %d", q.Len())
	}
}

func TestQueueThreadSafety(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 1000)
	var wg sync.WaitGroup

	numProducers := 10
	itemsPerProducer := 50

	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				_ = q.Push(ChunkTask{Index: pID*1000 + i})
			}
		}(p)
	}

	poppedCount := 0
	var popMu sync.Mutex
	numConsumers := 5

	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			for {
				_, ok := q.Next(ctx)
				if !ok {
					return
				}
				popMu.Lock()
				poppedCount++
				if poppedCount == numProducers*itemsPerProducer {
					q.Close()
				}
				popMu.Unlock()
			}
		}()
	}

	wg.Wait()

	popMu.Lock()
	finalPopped := poppedCount
	popMu.Unlock()

	if finalPopped != numProducers*itemsPerProducer {
		t.Fatalf("expected %d total popped, got %d", numProducers*itemsPerProducer, finalPopped)
	}
}

func TestQueueBlockingNextAndClose(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	doneCh := make(chan bool)
	go func() {
		ctx := context.Background()
		task, ok := q.Next(ctx)
		if ok || task.Index != 0 {
			t.Errorf("expected Next to return false on close, got ok=%v task=%v", ok, task)
		}
		doneCh <- true
	}()

	select {
	case <-doneCh:
		t.Fatal("Next unblocked unexpectedly early")
	case <-time.After(50 * time.Millisecond):
		// Expected: worker is waiting
	}

	q.Close()

	select {
	case <-doneCh:
		// Success: Close unblocked worker
	case <-time.After(1 * time.Second):
		t.Fatal("Next failed to unblock after Close()")
	}
}
