package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_CapacityAndNoRealloc(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}
	capacity := 3
	q := NewChunkQueue(initialTasks, capacity)

	if q.Cap() != capacity {
		t.Fatalf("expected cap %d, got %d", capacity, q.Cap())
	}
	if q.Len() != 2 {
		t.Fatalf("expected len 2, got %d", q.Len())
	}

	// Push 3rd task - should succeed
	task3 := ChunkTask{Index: 2, ChunkID: core.ChunkID{3}}
	if !q.Push(task3) {
		t.Fatalf("expected Push to succeed for 3rd item")
	}
	if q.Len() != capacity {
		t.Fatalf("expected len %d, got %d", capacity, q.Len())
	}

	// Push 4th task - should fail due to capacity limit
	task4 := ChunkTask{Index: 3, ChunkID: core.ChunkID{4}}
	if q.Push(task4) {
		t.Fatalf("expected Push to fail when queue is at capacity")
	}

	// Pop 1 item
	ctx := context.Background()
	item, ok := q.Next(ctx)
	if !ok || item.Index != 0 {
		t.Fatalf("expected item 0, got ok=%v, item=%v", ok, item)
	}

	// Push 4th task now - should succeed after space freed up
	if !q.Push(task4) {
		t.Fatalf("expected Push to succeed after pop")
	}

	// Verify ring buffer pop sequence
	expectedIndices := []int{1, 2, 3}
	for _, expected := range expectedIndices {
		task, ok := q.Next(ctx)
		if !ok || task.Index != expected {
			t.Fatalf("expected task index %d, got ok=%v, task index=%d", expected, ok, task.Index)
		}
	}

	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got len %d", q.Len())
	}
}

func TestChunkQueue_NextBlockingAndNotification(t *testing.T) {
	q := NewChunkQueue(nil, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var poppedChunk ChunkTask
	var poppedOk bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		poppedChunk, poppedOk = q.Next(ctx)
	}()

	// Give goroutine time to start waiting
	time.Sleep(20 * time.Millisecond)

	task := ChunkTask{Index: 42, ChunkID: core.ChunkID{99}}
	if !q.Push(task) {
		t.Fatalf("Push failed")
	}

	wg.Wait()

	if !poppedOk || poppedChunk.Index != 42 {
		t.Fatalf("expected popped task index 42, got ok=%v, chunk=%v", poppedOk, poppedChunk)
	}
}

func TestChunkQueue_CloseUnblocks(t *testing.T) {
	q := NewChunkQueue(nil, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	doneCh := make(chan bool)
	go func() {
		_, ok := q.Next(ctx)
		doneCh <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case ok := <-doneCh:
		if ok {
			t.Fatalf("expected ok=false after queue close")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for q.Next to unblock on close")
	}
}
