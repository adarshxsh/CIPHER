package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueue_CapacityAndOverflow(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}
	q := NewChunkQueueWithCapacity(initialTasks, 3)

	if q.Capacity() != 3 {
		t.Fatalf("expected capacity 3, got %d", q.Capacity())
	}
	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}

	// Adding 3rd task should succeed
	err := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if err != nil {
		t.Fatalf("expected push to succeed, got %v", err)
	}

	// Adding 4th task should overflow and return ErrQueueFull
	err = q.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{4}})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestChunkQueue_NextForPeer_SkipsMissed(t *testing.T) {
	tasks := []ChunkTask{
		{
			Index:       0,
			ChunkID:     core.ChunkID{1},
			MissedPeers: map[string]bool{"peerA": true},
		},
		{
			Index:   1,
			ChunkID: core.ChunkID{2},
		},
	}
	q := NewChunkQueueWithCapacity(tasks, 10)

	// PeerA asks for next task; should skip task 0 and receive task 1
	task, ok := q.NextForPeer(context.Background(), "peerA")
	if !ok {
		t.Fatalf("expected to get task for peerA")
	}
	if task.Index != 1 {
		t.Fatalf("expected task index 1, got %d", task.Index)
	}

	// PeerB asks for next task; task 0 is eligible for PeerB
	taskB, ok := q.NextForPeer(context.Background(), "peerB")
	if !ok {
		t.Fatalf("expected to get task for peerB")
	}
	if taskB.Index != 0 {
		t.Fatalf("expected task index 0, got %d", taskB.Index)
	}
}

func TestChunkQueue_Close(t *testing.T) {
	q := NewChunkQueueWithCapacity([]ChunkTask{}, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		_, ok := q.NextForPeer(ctx, "peerA")
		done <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	q.Close()

	ok := <-done
	if ok {
		t.Fatalf("expected NextForPeer to return false after Close")
	}
}
