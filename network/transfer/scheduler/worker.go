package scheduler

import (
	"context"
	"time"

	"cipher/network/content/engine"
	"cipher/network/protocol/chunk"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task             ChunkTask
	Error            error
	PeerID           string // To track contribution
	BytesTransferred int64
	Duration         time.Duration
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	for {
		if ctx.Err() != nil {
			return
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[source.PeerID.String()] {
			queue.Push(task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}:
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
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String(), BytesTransferred: int64(len(chunkData.Data)), Duration: dur}:
		}
	}
}
