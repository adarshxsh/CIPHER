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
		if rep != nil {
			if rep.IsBlacklisted(peerIDStr) {
				return
			}
			if rep.IsInCooldown(peerIDStr) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					continue
				}
			}
		}

		task, ok := queue.Next()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			task, ok = queue.Next()
			if !ok {
				return // Queue empty
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
			if rep != nil && rep.IsBlacklisted(peerIDStr) {
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr}
			if rep != nil && rep.IsBlacklisted(peerIDStr) {
				return
			}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr}
	}
}
