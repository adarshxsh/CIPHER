package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func dummyChunkID(b byte) core.ChunkID {
	var id core.ChunkID
	id[0] = b
	return id
}

func TestChunkQueue_CapacityBounds(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: dummyChunkID(1)},
		{Index: 1, ChunkID: dummyChunkID(2)},
	}

	queue := NewChunkQueue(initialTasks, 2)
	if queue.Cap() != 2 {
		t.Fatalf("expected capacity 2, got %d", queue.Cap())
	}
	if queue.Len() != 2 {
		t.Fatalf("expected length 2, got %d", queue.Len())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	pushed := make(chan bool, 1)
	go func() {
		err := queue.Push(ctx, ChunkTask{Index: 2, ChunkID: dummyChunkID(3)})
		if err == nil {
			pushed <- true
		}
	}()

	select {
	case <-pushed:
		t.Fatalf("expected Push to block when queue is full")
	case <-time.After(50 * time.Millisecond):
		// Expected: Push is blocking
	}

	// Pop one item, allowing Push to unblock
	task, ok := queue.Next(ctx)
	if !ok || task.Index != 0 {
		t.Fatalf("expected task index 0, got task %+v, ok %v", task, ok)
	}

	select {
	case <-pushed:
		// Successfully unblocked
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected Push to unblock after item popped")
	}

	if queue.Len() != 2 {
		t.Fatalf("expected length 2 after push unblocked, got %d", queue.Len())
	}
}

func TestChunkQueue_ContextCancellation(t *testing.T) {
	queue := NewChunkQueue([]ChunkTask{{Index: 0, ChunkID: dummyChunkID(1)}}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := queue.Push(ctx, ChunkTask{Index: 1, ChunkID: dummyChunkID(2)})
	if err == nil {
		t.Fatalf("expected error on canceled context Push")
	}
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestChunkQueue_CloseUnblocksWaitingWorkers(t *testing.T) {
	queue := NewChunkQueue(nil, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		_, ok := queue.Next(ctx)
		done <- ok
	}()

	time.Sleep(30 * time.Millisecond)
	queue.Close()

	select {
	case ok := <-done:
		if ok {
			t.Fatalf("expected ok=false after queue closed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected worker to unblock on queue close")
	}
}

func TestWorker_ErrorThrottling(t *testing.T) {
	origBackoff := WorkerErrorBackoff
	WorkerErrorBackoff = 100 * time.Millisecond
	defer func() { WorkerErrorBackoff = origBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := throttleWorker(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error from throttleWorker: %v", err)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected throttle duration >= 80ms, got %v", elapsed)
	}
}

func TestScheduler_ExponentialBackoffTiming(t *testing.T) {
	tasks := []ChunkTask{{Index: 0, ChunkID: dummyChunkID(1), Attempts: 0}}
	queue := NewChunkQueue(tasks, 10)
	defer queue.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Initial pop
	task, ok := queue.Next(ctx)
	if !ok {
		t.Fatalf("failed to pop task")
	}

	initialBackoff := 50 * time.Millisecond

	// Attempt 1 retry (Attempts will become 1, backoff = 50ms * 2^0 = 50ms)
	task.Attempts++
	backoff1 := initialBackoff * time.Duration(1<<(task.Attempts-1))
	if backoff1 != 50*time.Millisecond {
		t.Fatalf("expected backoff1 to be 50ms, got %v", backoff1)
	}

	start := time.Now()
	requeueDone := make(chan error, 1)
	go func() {
		timer := time.NewTimer(backoff1)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			requeueDone <- ctx.Err()
		case <-timer.C:
			requeueDone <- queue.Push(ctx, task)
		}
	}()

	if err := <-requeueDone; err != nil {
		t.Fatalf("requeue failed: %v", err)
	}
	elapsed1 := time.Since(start)
	if elapsed1 < 40*time.Millisecond {
		t.Fatalf("expected backoff >= 40ms, got %v", elapsed1)
	}

	// Attempt 2 retry (Attempts will become 2, backoff = 50ms * 2^1 = 100ms)
	taskPopped, ok := queue.Next(ctx)
	if !ok {
		t.Fatalf("failed to pop task after 1st retry")
	}

	taskPopped.Attempts++
	backoff2 := initialBackoff * time.Duration(1<<(taskPopped.Attempts-1))
	if backoff2 != 100*time.Millisecond {
		t.Fatalf("expected backoff2 to be 100ms, got %v", backoff2)
	}

	start = time.Now()
	go func() {
		timer := time.NewTimer(backoff2)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			requeueDone <- ctx.Err()
		case <-timer.C:
			requeueDone <- queue.Push(ctx, taskPopped)
		}
	}()

	if err := <-requeueDone; err != nil {
		t.Fatalf("requeue failed: %v", err)
	}
	elapsed2 := time.Since(start)
	if elapsed2 < 80*time.Millisecond {
		t.Fatalf("expected backoff >= 80ms, got %v", elapsed2)
	}
}

func TestScheduler_AsyncCompletions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	compChan := make(chan WorkerResult, 100)
	completions := make(chan WorkerResult) // Unbuffered downstream consumer
	compDone := make(chan struct{})

	go func() {
		defer close(compDone)
		var buffer []WorkerResult
		out := completions

		for {
			var current WorkerResult
			var activeOut chan<- WorkerResult

			if len(buffer) > 0 {
				current = buffer[0]
				activeOut = out
			}

			select {
			case res, ok := <-compChan:
				if !ok {
					if out != nil {
						for _, item := range buffer {
							select {
							case <-ctx.Done():
								return
							case out <- item:
							}
						}
					}
					return
				}
				buffer = append(buffer, res)
			case activeOut <- current:
				buffer = buffer[1:]
			case <-ctx.Done():
				return
			}
		}
	}()

	// Push 10 completions without reading from 'completions'
	for i := 0; i < 10; i++ {
		compChan <- WorkerResult{Task: ChunkTask{Index: i}}
	}
	close(compChan)

	// Now read 10 items from completions
	received := 0
	for i := 0; i < 10; i++ {
		select {
		case <-completions:
			received++
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("timeout reading completion %d", i)
		}
	}

	if received != 10 {
		t.Fatalf("expected 10 completions, got %d", received)
	}

	select {
	case <-compDone:
		// Clean exit
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("completion dispatcher did not terminate cleanly")
	}
}
