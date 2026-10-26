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
	peerID := source.PeerID.String()
	for {
		task, ok := queue.Next(ctx, peerID)
		if !ok {
			return // Queue empty, closed, or peer missed all tasks
		}
		
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
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
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
			case <-ctx.Done():
				return
			}
			continue
		}

		select {
		case results <- WorkerResult{Task: task, Error: nil, PeerID: peerID}:
		case <-ctx.Done():
			return
		}
	}
}
