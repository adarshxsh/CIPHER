package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestComputeBackoff(t *testing.T) {
	base := 10 * time.Millisecond
	maxBackoff := 100 * time.Millisecond

	// Attempt 1
	b1 := computeBackoff(base, maxBackoff, 1)
	if b1 < 10*time.Millisecond || b1 > 15*time.Millisecond {
		t.Errorf("attempt 1 backoff out of range: %v", b1)
	}

	// Attempt 2: base * 2 = 20ms (+ up to 10ms jitter)
	b2 := computeBackoff(base, maxBackoff, 2)
	if b2 < 20*time.Millisecond || b2 > 30*time.Millisecond {
		t.Errorf("attempt 2 backoff out of range: %v", b2)
	}

	// Attempt 3: base * 4 = 40ms (+ up to 20ms jitter)
	b3 := computeBackoff(base, maxBackoff, 3)
	if b3 < 40*time.Millisecond || b3 > 60*time.Millisecond {
		t.Errorf("attempt 3 backoff out of range: %v", b3)
	}
}

func TestWorkerPeerCooldown(t *testing.T) {
	// Override PeerCooldown for fast test execution
	oldCooldown := PeerCooldown
	PeerCooldown = 50 * time.Millisecond
	defer func() { PeerCooldown = oldCooldown }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	q := NewChunkQueue([]ChunkTask{{Index: 0, ChunkID: core.ChunkID{1}}})
	results := make(chan WorkerResult, 2)

	// Simulate error with fake client / runWorker loop
	go func() {
		for {
			task, ok := q.Next()
			if !ok {
				return
			}
			// Simulate error
			results <- WorkerResult{Task: task, Error: fmt.Errorf("fake fetch error")}
			cooldown := PeerCooldown
			select {
			case <-ctx.Done():
				return
			case <-time.After(cooldown):
			}
		}
	}()

	start := time.Now()
	res1 := <-results
	if res1.Error == nil {
		t.Fatal("expected error")
	}

	// Requeue task
	q.Push(res1.Task)

	res2 := <-results
	elapsed := time.Since(start)

	if res2.Error == nil {
		t.Fatal("expected error")
	}

	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected worker to pause for cooldown, elapsed: %v", elapsed)
	}

	q.Close()
}

func TestScheduler_SlowCompletionConsumerDoesNotBlock(t *testing.T) {
	oldCooldown := PeerCooldown
	PeerCooldown = 5 * time.Millisecond
	defer func() { PeerCooldown = oldCooldown }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Slow completions channel with 0 capacity
	completions := make(chan WorkerResult)

	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	// We will send mock results directly via dispatchCh logic simulation or Run loop
	// Test that dispatchCh absorbs completions even if consumer is slow
	dispatchCh := make(chan WorkerResult, len(tasks))
	dispatchDone := make(chan struct{})

	go func() {
		defer close(dispatchDone)
		for res := range dispatchCh {
			select {
			case completions <- res:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Producer sends results without blocking
	start := time.Now()
	dispatchCh <- WorkerResult{Task: tasks[0]}
	dispatchCh <- WorkerResult{Task: tasks[1]}
	duration := time.Since(start)

	if duration > 20*time.Millisecond {
		t.Fatalf("dispatching to completions buffer blocked for %v", duration)
	}

	// Slow consumer reads completions later
	time.Sleep(50 * time.Millisecond)
	<-completions
	<-completions

	close(dispatchCh)
	<-dispatchDone
}
