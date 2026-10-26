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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, tracker *ReputationTracker) {
	peerID := source.PeerID.String()
	for {
		if tracker != nil && tracker.IsQuarantined(peerID) {
			return // Worker linked to quarantined peer exits immediately and releases resources
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		if tracker != nil && tracker.IsQuarantined(peerID) {
			queue.Push(task)
			return // Peer quarantined while waiting on queue, requeue task and exit immediately
		}

		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[peerID] {
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
			results <- WorkerResult{Task: task, Error: err, PeerID: peerID}
			if tracker != nil && tracker.IsQuarantined(peerID) {
				return // Peer quarantined after error, exit immediately
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerID}
			if tracker != nil && tracker.IsQuarantined(peerID) {
				return // Peer quarantined after error, exit immediately
			}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerID}
	}
}
