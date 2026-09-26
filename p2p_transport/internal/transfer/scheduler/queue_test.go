package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueueCapacityBounds(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	// Default capacity for 2 initial tasks should be max(2*3, 10) = 10
	q := NewChunkQueue(initialTasks)
	if q.Capacity() != 10 {
		t.Fatalf("expected capacity 10, got %d", q.Capacity())
	}

	// Explicit capacity
	boundedQ := NewChunkQueueWithCapacity(initialTasks, 3)
	if boundedQ.Capacity() != 3 {
		t.Fatalf("expected capacity 3, got %d", boundedQ.Capacity())
	}

	// Fill to capacity
	err := boundedQ.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if err != nil {
		t.Fatalf("unexpected error pushing task to bounded queue: %v", err)
	}

	// Next push should exceed capacity and return ErrQueueFull
	err = boundedQ.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{4}})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got: %v", err)
	}
}

func TestCalculateBackoffBoundaries(t *testing.T) {
	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{attempts: 0, expected: 10 * time.Millisecond},
		{attempts: 1, expected: 10 * time.Millisecond},
		{attempts: 2, expected: 20 * time.Millisecond},
		{attempts: 3, expected: 40 * time.Millisecond},
		{attempts: 4, expected: 80 * time.Millisecond},
		{attempts: 7, expected: 640 * time.Millisecond},
		{attempts: 8, expected: 1 * time.Second},
		{attempts: 10, expected: 1 * time.Second},
		{attempts: 100, expected: 1 * time.Second},
	}

	for _, tt := range tests {
		delay := CalculateBackoff(tt.attempts)
		if delay != tt.expected {
			t.Errorf("CalculateBackoff(%d) = %v, expected %v", tt.attempts, delay, tt.expected)
		}
	}
}

func TestChunkQueueExponentialBackoffDelay(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	backoffDuration := 50 * time.Millisecond
	availTime := time.Now().Add(backoffDuration)

	task := ChunkTask{
		Index:       0,
		ChunkID:     core.ChunkID{1},
		Attempts:    1,
		AvailableAt: availTime,
	}

	if err := q.Push(task); err != nil {
		t.Fatalf("failed to push task: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	retrievedTask, ok := q.Next(ctx, "peer1")
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("expected task retrieval to succeed after backoff")
	}

	if retrievedTask.Index != 0 {
		t.Fatalf("expected task index 0, got %d", retrievedTask.Index)
	}

	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected next to delay at least ~40ms for backoff, got %v", elapsed)
	}
}

func TestPeerAwareTaskDispatchNoSpinLoop(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	missedTask := ChunkTask{
		Index:   0,
		ChunkID: core.ChunkID{1},
		MissedPeers: map[string]bool{
			"peerA": true,
		},
	}

	if err := q.Push(missedTask); err != nil {
		t.Fatalf("failed to push task: %v", err)
	}

	ctx := context.Background()

	// peerA should NOT receive missedTask
	_, ok := q.Next(ctx, "peerA")
	if ok {
		t.Fatal("peerA should not have retrieved a task it missed")
	}

	// Queue should still hold missedTask for peerB
	if q.Len() != 1 {
		t.Fatalf("expected queue length 1, got %d", q.Len())
	}

	// peerB SHOULD receive missedTask
	taskForB, ok := q.Next(ctx, "peerB")
	if !ok {
		t.Fatal("peerB should have retrieved task")
	}
	if taskForB.Index != 0 {
		t.Fatalf("expected index 0, got %d", taskForB.Index)
	}

	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got %d", q.Len())
	}
}

func TestChunkQueueCloseAndContextCancellation(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, ok := q.Next(ctx, "peer1")
	if ok {
		t.Fatal("expected Next to return false when context is cancelled")
	}

	q.Close()
	_, ok = q.Next(context.Background(), "peer1")
	if ok {
		t.Fatal("expected Next to return false when queue is closed")
	}
}
