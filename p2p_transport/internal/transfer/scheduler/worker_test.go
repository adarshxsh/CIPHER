package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestRunWorkerContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create queue with tasks
	task := ChunkTask{
		Index:   0,
		ChunkID: core.ChunkID{1, 2, 3},
	}
	queue := NewChunkQueue([]ChunkTask{task})

	// Unbuffered results channel that is not being read
	results := make(chan WorkerResult)

	source := Source{
		Available: nil,
	}

	done := make(chan struct{})
	go func() {
		// client and eng can be nil since queue item will try client.FetchChunk which panics if called, or
		// if we cancel context first, runWorker returns cleanly without blocking on results.
		runWorker(ctx, source, nil, nil, queue, results)
		close(done)
	}()

	// Cancel context immediately
	cancel()

	select {
	case <-done:
		// Success: runWorker exited cleanly
	case <-time.After(1 * time.Second):
		t.Fatal("runWorker blocked on context cancellation")
	}
}

func TestRunWorkerContextCanceledDuringSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Unbuffered channel
	results := make(chan WorkerResult)

	task := ChunkTask{
		Index:   0,
		ChunkID: core.ChunkID{1, 2, 3},
	}
	queue := NewChunkQueue([]ChunkTask{task})

	source := Source{}

	done := make(chan struct{})
	go func() {
		// client.FetchChunk will panic if client is nil and ctx is active,
		// so let's cancel context right after worker starts.
		runWorker(ctx, source, nil, nil, queue, results)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// Worker returned cleanly upon context cancellation
	case <-time.After(2 * time.Second):
		t.Fatal("Worker goroutine hung on canceled context")
	}
}
