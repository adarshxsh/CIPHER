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
	sendResult := func(res WorkerResult) bool {
		select {
		case <-ctx.Done():
			return false
		case results <- res:
			return true
		}
	}

	for {
		task, ok := queue.NextCtx(ctx)
		if !ok {
			return // Queue closed or context cancelled
		}

		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[source.PeerID.String()] {
			if err := queue.PushCtx(ctx, task); err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			if !sendResult(WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}) {
				return
			}
			continue
		}

		if TestThrottle > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(TestThrottle):
			}
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if !sendResult(WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}) {
				return
			}
			continue
		}

		if !sendResult(WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}) {
			return
		}
	}
}
