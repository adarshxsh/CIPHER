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
	}

	// Test custom capacity
	q := NewChunkQueueWithCapacity(tasks, 3)
	if q.Cap() != 3 {
		t.Fatalf("expected capacity 3, got %d", q.Cap())
	}
	if q.Len() != 2 {
		t.Fatalf("expected length 2, got %d", q.Len())
	}

	// Third push should succeed
	pushed := q.Push(ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	if !pushed {
		t.Fatalf("expected 3rd push to succeed")
	}

	// Fourth push should be rejected because capacity limit is 3
	pushed = q.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{4}})
	if pushed {
		t.Fatalf("expected 4th push to be rejected due to capacity limit")
	}

	if q.Len() != 3 {
		t.Fatalf("expected queue length to stay 3, got %d", q.Len())
	}

	// Pop one item
	task, ok := q.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got %v", task)
	}

	// Now push should succeed again
	pushed = q.Push(ChunkTask{Index: 3, ChunkID: core.ChunkID{4}})
	if !pushed {
		t.Fatalf("expected push to succeed after pop")
	}
}

func TestChunkQueue_DefaultCapacity(t *testing.T) {
	tasks := make([]ChunkTask, 10)
	q := NewChunkQueue(tasks)
	expectedCap := 30
	if q.Cap() != expectedCap {
		t.Fatalf("expected default capacity 3x task count (%d), got %d", expectedCap, q.Cap())
	}

	emptyQ := NewChunkQueue(nil)
	if emptyQ.Cap() != DefaultMaxQueueCapacity {
		t.Fatalf("expected empty queue capacity to be DefaultMaxQueueCapacity (%d), got %d", DefaultMaxQueueCapacity, emptyQ.Cap())
	}
}

func TestChunkQueue_NextForPeer(t *testing.T) {
	t1 := ChunkTask{Index: 0, MissedPeers: map[string]bool{"peer1": true}}
	t2 := ChunkTask{Index: 1, MissedPeers: map[string]bool{"peer2": true}}
	t3 := ChunkTask{Index: 2}

	q := NewChunkQueueWithCapacity([]ChunkTask{t1, t2, t3}, 10)

	// peer1 should skip t1 and get t2
	task, ok := q.NextForPeer("peer1")
	if !ok || task.Index != 1 {
		t.Fatalf("expected peer1 to get task 1, got ok=%v task=%+v", ok, task)
	}

	// peer2 should get t1 (since t2 was popped, and peer2 hasn't missed t1)
	task, ok = q.NextForPeer("peer2")
	if !ok || task.Index != 0 {
		t.Fatalf("expected peer2 to get task 0, got ok=%v task=%+v", ok, task)
	}

	// peer1 should get t3
	task, ok = q.NextForPeer("peer1")
	if !ok || task.Index != 2 {
		t.Fatalf("expected peer1 to get task 2, got ok=%v task=%+v", ok, task)
	}

	// Queue should now be empty
	_, ok = q.NextForPeer("peer1")
	if ok {
		t.Fatalf("expected queue to be empty")
	}
}

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{0, 10 * time.Millisecond},
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 160 * time.Millisecond},
		{10, 1 * time.Second}, // Capped at 1s
		{20, 1 * time.Second}, // Capped at 1s
	}

	for _, tt := range tests {
		got := calculateBackoff(tt.attempts)
		if got != tt.expected {
			t.Errorf("calculateBackoff(%d) = %v, expected %v", tt.attempts, got, tt.expected)
		}
	}
}

func TestCompletionBuffer_NonBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	completions := make(chan WorkerResult) // Unbuffered completion channel
	compBuf := startCompletionBuffer(ctx, completions)

	start := time.Now()
	for i := 0; i < 100; i++ {
		compBuf.Push(WorkerResult{Task: ChunkTask{Index: i}})
	}
	duration := time.Since(start)

	if duration > 10*time.Millisecond {
		t.Fatalf("pushing 100 items to completion buffer blocked! took %v", duration)
	}

	// Now consume completions slowly
	receivedCount := 0
	for i := 0; i < 100; i++ {
		res := <-completions
		if res.Task.Index != i {
			t.Fatalf("expected task index %d, got %d", i, res.Task.Index)
		}
		receivedCount++
	}

	compBuf.Close(ctx)

	if receivedCount != 100 {
		t.Fatalf("expected to receive 100 completions, got %d", receivedCount)
	}
}

func TestScheduler_RequeueBackoffTiming(t *testing.T) {
	// Verify that exponential backoff delay increases retry intervals
	task := ChunkTask{Index: 0, Attempts: 1}

	q := NewChunkQueueWithCapacity(nil, 10)

	start := time.Now()

	// Attempt 1 backoff (10ms)
	backoff1 := calculateBackoff(task.Attempts)
	go func() {
		time.Sleep(backoff1)
		q.Push(task)
	}()

	_, ok := q.Next()
	if ok {
		t.Fatalf("expected queue to be empty immediately")
	}

	time.Sleep(15 * time.Millisecond)
	taskPopped, ok := q.Next()
	if !ok || taskPopped.Index != 0 {
		t.Fatalf("expected task to be available after backoff 1")
	}

	// Attempt 2 backoff (20ms)
	task.Attempts++
	backoff2 := calculateBackoff(task.Attempts)
	if backoff2 != 20*time.Millisecond {
		t.Fatalf("expected 20ms backoff, got %v", backoff2)
	}

	t.Logf("Backoff test passed in %v", time.Since(start))
}
