package scheduler

import (
	"context"
	"log"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task   ChunkTask
	Error  error
	PeerID string // To track contribution
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, sm *reputation.ScoreManager) {
	for {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if sm != nil && sm.IsBanned(ctx, source.PeerID) {
			log.Printf("[Worker] Peer %s is banned, stopping worker", source.PeerID)
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

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			if sm != nil {
				sm.RecordError(ctx, source.PeerID, err)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			if sm != nil && sm.IsBanned(ctx, source.PeerID) {
				log.Printf("[Worker] Peer %s reached ban threshold after chunk fetch error, stopping worker", source.PeerID)
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if sm != nil {
				sm.RecordError(ctx, source.PeerID, err)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			if sm != nil && sm.IsBanned(ctx, source.PeerID) {
				log.Printf("[Worker] Peer %s reached ban threshold after chunk storage error, stopping worker", source.PeerID)
				return
			}
			continue
		}

		if sm != nil {
			sm.RecordSuccess(ctx, source.PeerID)
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
