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
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if !sendResult(WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}) {
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
			if !sendResult(WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}) {
				return
			}
			continue
		}

		if !sendResult(WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String(), BytesTransferred: int64(len(chunkData.Data)), Duration: dur}) {
			return
		}
	}
}
