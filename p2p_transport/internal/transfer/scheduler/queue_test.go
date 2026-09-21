package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_BasicPushPop(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{0x1}},
		{Index: 1, ChunkID: core.ChunkID{0x2}},
	}
	q := NewChunkQueue(10, tasks)

	if q.Len() != 2 {
		t.Fatalf("expected queue length 2, got %d", q.Len())
	}
	if q.Cap() != 10 {
		t.Fatalf("expected queue capacity 10, got %d", q.Cap())
	}

	ctx := context.Background()
	task1, ok := q.Pop(ctx)
	if !ok || task1.Index != 0 {
		t.Fatalf("expected task index 0, got %v (ok=%v)", task1, ok)
	}

	task2, ok := q.Pop(ctx)
	if !ok || task2.Index != 1 {
		t.Fatalf("expected task index 1, got %v (ok=%v)", task2, ok)
	}

	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got len %d", q.Len())
	}
}

func TestChunkQueue_CapacityBounds(t *testing.T) {
	q := NewChunkQueue(2, nil)

	if !q.TryPush(ChunkTask{Index: 0}) {
		t.Fatal("expected TryPush 0 to succeed")
	}
	if !q.TryPush(ChunkTask{Index: 1}) {
		t.Fatal("expected TryPush 1 to succeed")
	}
	if q.TryPush(ChunkTask{Index: 2}) {
		t.Fatal("expected TryPush 2 to fail because queue is full")
	}

	if q.Len() != 2 {
		t.Fatalf("expected queue len 2, got %d", q.Len())
	}

	// Unblock by popping
	ctx := context.Background()
	_, ok := q.Pop(ctx)
	if !ok {
		t.Fatal("expected Pop to succeed")
	}

	if !q.TryPush(ChunkTask{Index: 2}) {
		t.Fatal("expected TryPush 2 to succeed after pop")
	}
}

func TestChunkQueue_WorkerWaitOnEmpty(t *testing.T) {
	q := NewChunkQueue(5, nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	var poppedTask ChunkTask
	var poppedOk bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		poppedTask, poppedOk = q.Pop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	pushed := q.TryPush(ChunkTask{Index: 42})
	if !pushed {
		t.Fatal("expected TryPush to succeed")
	}

	wg.Wait()

	if !poppedOk || poppedTask.Index != 42 {
		t.Fatalf("expected popped task index 42, got %v (ok=%v)", poppedTask, poppedOk)
	}
}

func TestChunkQueue_CloseUnblocks(t *testing.T) {
	q := NewChunkQueue(5, nil)
	ctx := context.Background()

	done := make(chan bool)
	go func() {
		_, ok := q.Pop(ctx)
		done <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected Pop to return false after Close")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Pop to unblock on Close")
	}
}

func TestChunkQueue_PushBlocksWhenFullUntilPopped(t *testing.T) {
	q := NewChunkQueue(2, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = q.Push(ctx, ChunkTask{Index: 1})
	_ = q.Push(ctx, ChunkTask{Index: 2})

	pushed := make(chan error, 1)
	go func() {
		pushed <- q.Push(ctx, ChunkTask{Index: 3})
	}()

	select {
	case err := <-pushed:
		t.Fatalf("Push should have blocked on full queue, returned: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Expected blocking
	}

	_, _ = q.Pop(ctx) // Pop 1 element

	select {
	case err := <-pushed:
		if err != nil {
			t.Fatalf("Push failed with error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Push did not unblock after Pop")
	}
}

func TestChunkQueue_RingBufferWrapAround(t *testing.T) {
	q := NewChunkQueue(3, nil)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		err := q.Push(ctx, ChunkTask{Index: i})
		if err != nil {
			t.Fatalf("Push %d failed: %v", i, err)
		}
		task, ok := q.Pop(ctx)
		if !ok || task.Index != i {
			t.Fatalf("Pop %d failed: got %v (ok=%v)", i, task, ok)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("expected len 0, got %d", q.Len())
	}
}
