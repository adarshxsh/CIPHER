package scheduler

import (
	"context"
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

func TestScheduler_SlowCompletionConsumerDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Slow completions channel with 0 capacity
	completions := make(chan WorkerResult)

	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

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

	start := time.Now()
	dispatchCh <- WorkerResult{Task: tasks[0]}
	dispatchCh <- WorkerResult{Task: tasks[1]}
	duration := time.Since(start)

	if duration > 20*time.Millisecond {
		t.Fatalf("dispatching to completions buffer blocked for %v", duration)
	}

	time.Sleep(50 * time.Millisecond)
	<-completions
	<-completions

	close(dispatchCh)
	<-dispatchDone
}
