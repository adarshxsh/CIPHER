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
		task, ok := queue.Pop(ctx)
		if !ok {
			return // Context done or queue closed
		}
		
		// If this source already returned candidate miss for this task, defer re-push using backoff delay
		if task.MissedPeers != nil && task.MissedPeers[source.PeerID.String()] {
			delay := CalculateBackoff(len(task.MissedPeers), 0, 0)
			go func(t ChunkTask, d time.Duration) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
					queue.PushCtx(ctx, t)
				}
			}(task, delay)
			continue
		}
		
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
