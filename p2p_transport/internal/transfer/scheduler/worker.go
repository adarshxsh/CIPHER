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
		task, ok := queue.PopForPeer(ctx, peerID)
		if !ok {
			return // Queue empty or context cancelled
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
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
			select {
			case <-ctx.Done():
				return
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case results <- WorkerResult{Task: task, Error: nil, PeerID: peerID}:
		}
	}
}
