package scheduler

import (
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_CapacityAndBlocking(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	q := NewChunkQueue(tasks, 2)
	if q.Capacity() != 2 {
		t.Fatalf("expected capacity 2, got %d", q.Capacity())
	}
	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}

	pushed := make(chan bool)
	go func() {
		// Queue is full (2/2). Push should block until Next is called.
		ok := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
		pushed <- ok
	}()

	select {
	case <-pushed:
		t.Fatal("Push should have blocked on full capacity")
	case <-time.After(50 * time.Millisecond):
		// Expected to block
	}

	// Pop one item
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("unexpected task from Next: got %v, ok %v", task, ok)
	}

	// Now Push should succeed
	select {
	case ok := <-pushed:
		if !ok {
			t.Fatal("expected Push to succeed after space was freed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Push did not unblock after Next freed space")
	}

	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}

	q.Close()
}

func TestChunkQueue_CloseUnblocks(t *testing.T) {
	qNext := NewChunkQueue(nil, 1)

	nextDone := make(chan bool)
	go func() {
		_, ok := qNext.Next()
		nextDone <- ok
	}()

	time.Sleep(10 * time.Millisecond)
	qNext.Close()

	select {
	case ok := <-nextDone:
		if ok {
			t.Error("expected Next to return false on closed empty queue")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Next did not unblock when queue closed")
	}

	qPush := NewChunkQueue(nil, 1)
	qPush.Push(ChunkTask{Index: 0}) // Fill capacity

	pushDone := make(chan bool)
	go func() {
		ok := qPush.Push(ChunkTask{Index: 1})
		pushDone <- ok
	}()

	time.Sleep(10 * time.Millisecond)
	qPush.Close()

	select {
	case ok := <-pushDone:
		if ok {
			t.Error("expected Push to return false when queue is closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Push did not unblock when queue closed")
	}
}
