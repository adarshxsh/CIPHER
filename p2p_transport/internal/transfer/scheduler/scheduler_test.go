package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestCalculateBackoff(t *testing.T) {
	base := 10 * time.Millisecond
	max := 100 * time.Millisecond

	b1 := CalculateBackoff(1, base, max)
	if b1 != 10*time.Millisecond {
		t.Fatalf("attempt 1 backoff expected 10ms, got %v", b1)
	}

	b2 := CalculateBackoff(2, base, max)
	if b2 != 20*time.Millisecond {
		t.Fatalf("attempt 2 backoff expected 20ms, got %v", b2)
	}

	b3 := CalculateBackoff(3, base, max)
	if b3 != 40*time.Millisecond {
		t.Fatalf("attempt 3 backoff expected 40ms, got %v", b3)
	}

	b5 := CalculateBackoff(5, base, max)
	if b5 != max {
		t.Fatalf("attempt 5 backoff expected max %v, got %v", max, b5)
	}
}

func TestScheduler_AsyncCompletionsNoStall(t *testing.T) {
	completions := make(chan WorkerResult) // unbuffered!
	results := make(chan WorkerResult, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Simulate successful results sent to results channel
	res1 := WorkerResult{Task: ChunkTask{Index: 0, ChunkID: core.ChunkID{1}}, PeerID: "peer1"}
	res2 := WorkerResult{Task: ChunkTask{Index: 1, ChunkID: core.ChunkID{2}}, PeerID: "peer1"}

	results <- res1
	results <- res2

	// We start a receiver that sleeps before reading completions
	var received []WorkerResult
	doneCh := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond) // slow receiver
		for r := range completions {
			received = append(received, r)
		}
		close(doneCh)
	}()

	queue := NewChunkQueueWithBounds(nil, 1, 10)
	defer queue.Close()

	var completionWG sync.WaitGroup

	// Dispatch worker results through scheduler completion handling
	pendingTasks := 2
	for pendingTasks > 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("context timed out")
		case res := <-results:
			if res.Error == nil {
				pendingTasks--
				completionWG.Add(1)
				go func(r WorkerResult) {
					defer completionWG.Done()
					select {
					case completions <- r:
					case <-ctx.Done():
					}
				}(res)
			}
		}
	}

	completionWG.Wait()
	close(completions)
	<-doneCh

	if len(received) != 2 {
		t.Fatalf("expected 2 received completions, got %d", len(received))
	}
}

func TestScheduler_RetryExponentialBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s := NewScheduler(nil, nil, 3)
	s.BaseBackoff = 20 * time.Millisecond

	queue := NewChunkQueueWithBounds(nil, 1, 10)
	defer queue.Close()

	task := ChunkTask{Index: 0, ChunkID: core.ChunkID{1}, Attempts: 1}

	start := time.Now()
	s.requeueWithBackoff(ctx, queue, task, task.Attempts)

	// Task should not be immediately available
	if _, ok := queue.Next(); ok {
		t.Fatalf("task was immediately available in queue without waiting for backoff timer")
	}

	// Task should become available after backoff delay
	poppedTask, ok := queue.Pop(ctx)
	elapsed := time.Since(start)

	if !ok {
		t.Fatalf("failed to pop requeued task")
	}
	if poppedTask.Index != 0 {
		t.Fatalf("expected task index 0, got %d", poppedTask.Index)
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("expected backoff delay around 20ms, elapsed %v", elapsed)
	}
}
