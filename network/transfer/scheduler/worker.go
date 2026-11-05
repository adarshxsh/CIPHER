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
var PeerCooldown time.Duration = 100 * time.Millisecond

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	for {
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
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			if PeerCooldown > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(PeerCooldown):
				}
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			if PeerCooldown > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(PeerCooldown):
				}
			}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String(), BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}
