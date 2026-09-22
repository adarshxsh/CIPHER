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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, tracker *reputation.PeerReputationTracker) {
	for {
		if tracker != nil && tracker.IsBanned(source.PeerID) {
			log.Printf("[Worker] Peer %s is banned, evicting worker goroutine", source.PeerID)
			return
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		if tracker != nil && tracker.IsBanned(source.PeerID) {
			log.Printf("[Worker] Peer %s became banned, returning task %x and evicting worker", source.PeerID, task.ChunkID)
			queue.Push(task)
			return
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
			if tracker != nil && tracker.IsBanned(source.PeerID) {
				log.Printf("[Worker] Peer %s banned after fetch failure, evicting worker", source.PeerID)
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			if tracker != nil && tracker.IsBanned(source.PeerID) {
				log.Printf("[Worker] Peer %s banned after engine store failure, evicting worker", source.PeerID)
				return
			}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
