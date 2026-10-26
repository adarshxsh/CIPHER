package scheduler

import (
	"context"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestQueueCapacityLimit(t *testing.T) {
	// Verify default max capacity constant
	if DefaultMaxQueueCapacity != 10000 {
		t.Fatalf("expected DefaultMaxQueueCapacity to be 10000, got %d", DefaultMaxQueueCapacity)
	}

	// Test NewChunkQueueWithCapacity with 5 slots
	maxCap := 5
	var initialTasks []ChunkTask
	for i := 0; i < 10; i++ {
		initialTasks = append(initialTasks, ChunkTask{Index: i, Attempts: 0})
	}

	queue := NewChunkQueueWithCapacity(initialTasks, maxCap)
	if queue.Cap() != maxCap {
		t.Fatalf("expected queue cap to be %d, got %d", maxCap, queue.Cap())
	}
	if queue.Len() != maxCap {
		t.Fatalf("expected initial queue depth to be capped at %d, got %d", maxCap, queue.Len())
	}
	if queue.Depth() != maxCap {
		t.Fatalf("expected Depth() metric to be %d, got %d", maxCap, queue.Depth())
	}

	// Push should fail when queue is full
	extraTask := ChunkTask{Index: 99, Attempts: 0}
	if queue.Push(extraTask) {
		t.Fatalf("expected Push to fail when queue is at capacity")
	}

	err := queue.PushWithError(extraTask)
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull when queue is full, got %v", err)
	}

	if queue.Len() > maxCap {
		t.Fatalf("queue slice length (%d) exceeded max capacity (%d)", queue.Len(), maxCap)
	}

	// Pop one item, then Push should succeed once
	task, ok := queue.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected to pop task 0, got task %v, ok=%v", task, ok)
	}
	if queue.Len() != maxCap-1 {
		t.Fatalf("expected queue depth to be %d, got %d", maxCap-1, queue.Len())
	}

	if !queue.Push(extraTask) {
		t.Fatalf("expected Push to succeed after space opened up")
	}
	if queue.Len() != maxCap {
		t.Fatalf("expected queue depth to be %d, got %d", maxCap, queue.Len())
	}
	if queue.Push(extraTask) {
		t.Fatalf("expected Push to fail after filling queue back up")
	}
}

func TestExponentialBackoffCalculation(t *testing.T) {
	sched := &Scheduler{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	}

	// Attempt 0 or negative -> 0
	if delay := sched.CalculateBackoff(0); delay != 0 {
		t.Fatalf("expected 0 backoff for 0 attempts, got %v", delay)
	}

	// Attempt 1 -> 10ms (10 * 2^0)
	if delay := sched.CalculateBackoff(1); delay != 10*time.Millisecond {
		t.Fatalf("expected 10ms for attempt 1, got %v", delay)
	}

	// Attempt 2 -> 20ms (10 * 2^1)
	if delay := sched.CalculateBackoff(2); delay != 20*time.Millisecond {
		t.Fatalf("expected 20ms for attempt 2, got %v", delay)
	}

	// Attempt 3 -> 40ms (10 * 2^2)
	if delay := sched.CalculateBackoff(3); delay != 40*time.Millisecond {
		t.Fatalf("expected 40ms for attempt 3, got %v", delay)
	}

	// Attempt 4 -> 80ms (10 * 2^3)
	if delay := sched.CalculateBackoff(4); delay != 80*time.Millisecond {
		t.Fatalf("expected 80ms for attempt 4, got %v", delay)
	}

	// Attempt 5 -> capped at MaxBackoff (100ms)
	if delay := sched.CalculateBackoff(5); delay != 100*time.Millisecond {
		t.Fatalf("expected max backoff 100ms for attempt 5, got %v", delay)
	}
}

func TestSchedulerRequeueAndMetric(t *testing.T) {
	sched := &Scheduler{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		MaxQueueCap: 2,
	}

	// Uninitialized queue depth should be 0
	if depth := sched.QueueDepth(); depth != 0 {
		t.Fatalf("expected initial QueueDepth to be 0, got %d", depth)
	}
	if depth := sched.GetQueueDepth(); depth != 0 {
		t.Fatalf("expected GetQueueDepth to be 0, got %d", depth)
	}

	// Attach queue manually to test Requeue and QueueDepth
	q := NewChunkQueueWithCapacity(nil, 2)
	sched.mu.Lock()
	sched.queue = q
	sched.mu.Unlock()

	ctx := context.Background()
	task1 := ChunkTask{Index: 1, Attempts: 1}
	start := time.Now()
	err := sched.Requeue(ctx, task1)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("requeue task 1 failed: %v", err)
	}
	if elapsed < 5*time.Millisecond {
		t.Fatalf("expected backoff delay of at least 5ms, got %v", elapsed)
	}
	if sched.QueueDepth() != 1 {
		t.Fatalf("expected queue depth 1, got %d", sched.QueueDepth())
	}

	task2 := ChunkTask{Index: 2, Attempts: 1}
	if err := sched.Requeue(ctx, task2); err != nil {
		t.Fatalf("requeue task 2 failed: %v", err)
	}
	if sched.QueueDepth() != 2 {
		t.Fatalf("expected queue depth 2, got %d", sched.QueueDepth())
	}

	// Requeue task 3 should fail because max capacity is 2
	task3 := ChunkTask{Index: 3, Attempts: 1}
	err = sched.Requeue(ctx, task3)
	if err == nil {
		t.Fatalf("expected error when requeuing into full queue, got nil")
	}
}

func TestSimulatedPersistentOutageMemoryBound(t *testing.T) {
	// Simulate persistent network outage with queue capping and exponential backoff
	sched := &Scheduler{
		MaxAttempts: 3,
		BaseBackoff: 1 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
		MaxQueueCap: 50,
	}

	q := NewChunkQueueWithCapacity(nil, 50)
	sched.mu.Lock()
	sched.queue = q
	sched.mu.Unlock()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	ctx := context.Background()
	var dummyChunk core.ChunkID
	copy(dummyChunk[:], []byte("01234567890123456789012345678901"))

	// Simulate repeated worker failures and requeues up to MaxAttempts limit
	for i := 0; i < 100; i++ {
		task := ChunkTask{
			Index:    i,
			ChunkID:  dummyChunk,
			Attempts: 0,
		}
		for task.Attempts < sched.MaxAttempts {
			task.Attempts++
			if task.Attempts < sched.MaxAttempts {
				_ = sched.Requeue(ctx, task)
				// Consume task from queue if available
				_, _ = q.Next()
			}
		}
		// Ensure queue size never exceeds capacity limit
		if q.Len() > sched.MaxQueueCap {
			t.Fatalf("Queue depth %d exceeded max capacity %d", q.Len(), sched.MaxQueueCap)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	t.Logf("Allocations before: %d bytes, after: %d bytes", m1.HeapAlloc, m2.HeapAlloc)
}
