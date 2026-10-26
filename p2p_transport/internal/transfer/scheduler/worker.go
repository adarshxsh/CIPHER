package scheduler

import (
	"context"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task   ChunkTask
	Error  error
	PeerID string // To track contribution
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	for {
		task, ok := queue.NextWithContext(ctx)
		if !ok {
			return // Queue closed or context cancelled
		}
		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[source.PeerID.String()] {
			_ = queue.PushWithContext(ctx, task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}

		if source.Available != nil {
			if _, has := source.Available[task.ChunkID]; !has {
				// We don't think this source has the chunk.
				// For now, we still try since discovery isn't fully robust.
			}
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				return
			}
			continue
		}

		if TestThrottle > 0 {
			select {
			case <-time.After(TestThrottle):
			case <-ctx.Done():
				return
			}
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				return
			}
			continue
		}

		select {
		case results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}:
		case <-ctx.Done():
			return
		}
	}
}
