package scheduler

import (
	"context"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/reputation"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task   ChunkTask
	Error  error
	PeerID string // To track contribution
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, rep *reputation.ReputationManager) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if rep != nil {
			if rep.IsQuarantined(source.PeerID) {
				return // Peer quarantined, stop worker
			}
			if rep.IsBackedOff(source.PeerID) {
				rem := rep.BackoffRemaining(source.PeerID)
				if rem > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(rem):
					}
					if rep.IsQuarantined(source.PeerID) {
						return
					}
				}
			}
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

		if source.Available != nil {
			if _, has := source.Available[task.ChunkID]; !has {
				// We don't think this source has the chunk.
				// For now, we still try since discovery isn't fully robust.
			}
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
			if rep != nil {
				rep.RecordTransientFailure(source.PeerID)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
