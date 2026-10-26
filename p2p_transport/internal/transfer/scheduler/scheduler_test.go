package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestCalculateBackoff(t *testing.T) {
	base := 10 * time.Millisecond

	// Attempt 1: 10ms * 2^0 = 10ms
	if d := calculateBackoff(base, 1); d != 10*time.Millisecond {
		t.Errorf("expected 10ms for attempt 1, got %v", d)
	}

	// Attempt 2: 10ms * 2^1 = 20ms
	if d := calculateBackoff(base, 2); d != 20*time.Millisecond {
		t.Errorf("expected 20ms for attempt 2, got %v", d)
	}

	// Attempt 3: 10ms * 2^2 = 40ms
	if d := calculateBackoff(base, 3); d != 40*time.Millisecond {
		t.Errorf("expected 40ms for attempt 3, got %v", d)
	}

	// Attempt 4: 10ms * 2^3 = 80ms
	if d := calculateBackoff(base, 4); d != 80*time.Millisecond {
		t.Errorf("expected 80ms for attempt 4, got %v", d)
	}
}

func TestScheduler_ExponentialBackoffRetry(t *testing.T) {
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
	}

	q := NewChunkQueue(tasks, 10)
	baseDelay := 50 * time.Millisecond

	task, ok := q.Next()
	if !ok {
		t.Fatalf("expected task in queue")
	}

	task.Attempts++ // attempt 1 failure
	delay := calculateBackoff(baseDelay, task.Attempts)
	if delay != 50*time.Millisecond {
		t.Fatalf("expected 50ms delay for attempt 1, got %v", delay)
	}

	start := time.Now()
	ctx := context.Background()

	// Push task asynchronously after backoff delay
	go func() {
		time.Sleep(delay)
		_ = q.Push(task)
	}()

	requeuedTask, ok := q.Pop(ctx)
	elapsed := time.Since(start)

	if !ok {
		t.Fatalf("expected task after backoff")
	}
	if requeuedTask.Index != 0 || requeuedTask.Attempts != 1 {
		t.Fatalf("unexpected task state: %+v", requeuedTask)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("requeued task was available too early: %v", elapsed)
	}
}

func TestScheduler_SlowCompletionsDispatch(t *testing.T) {
	// Create unbuffered completions channel to test slow consumer handling
	completions := make(chan WorkerResult)

	// Simulate pending completions processing logic
	var pendingCompletions []WorkerResult
	pendingCompletions = append(pendingCompletions, WorkerResult{
		Task: ChunkTask{Index: 0, ChunkID: core.ChunkID{1}},
	})
	pendingCompletions = append(pendingCompletions, WorkerResult{
		Task: ChunkTask{Index: 1, ChunkID: core.ChunkID{2}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	received := make([]WorkerResult, 0, 2)
	done := make(chan struct{})

	// Consumer reads with artificial delay
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			time.Sleep(30 * time.Millisecond)
			select {
			case res := <-completions:
				received = append(received, res)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Event loop dispatching from pendingCompletions
	for len(pendingCompletions) > 0 {
		var sendChan chan<- WorkerResult
		var head WorkerResult
		if len(pendingCompletions) > 0 {
			sendChan = completions
			head = pendingCompletions[0]
		}

		select {
		case <-ctx.Done():
			t.Fatalf("dispatch timed out")
		case sendChan <- head:
			pendingCompletions = pendingCompletions[1:]
		}
	}

	<-done
	if len(received) != 2 {
		t.Fatalf("expected 2 completions, got %d", len(received))
	}
}
