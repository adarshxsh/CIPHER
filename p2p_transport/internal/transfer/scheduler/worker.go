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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, rep *ReputationManager) {
	peerIDStr := source.PeerID.String()
	for {
		if rep != nil && rep.IsBanned(peerIDStr) {
			return
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		if rep != nil && rep.IsBanned(peerIDStr) {
			queue.Push(task)
			return
		}

		if source.Available != nil {
			if _, has := source.Available[task.ChunkID]; !has {
				// We don't think this source has the chunk.
				// For now, we still try since discovery isn't fully robust.
			}
		}

		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[peerIDStr] {
			queue.Push(task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr}
	}
}
