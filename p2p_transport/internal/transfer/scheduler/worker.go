package scheduler

import (
	"context"
	"errors"
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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, tracker *PeerTracker, results chan<- WorkerResult) {
	for {
		if tracker.IsQuarantined(source.PeerID) {
			return
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		if tracker.IsQuarantined(source.PeerID) {
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

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			if !errors.Is(err, chunk.ErrRemoteChunkNotFound) {
				_, isQuarantined := tracker.RecordFault(source.PeerID)
				results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
				if isQuarantined {
					return
				}
				continue
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			_, isQuarantined := tracker.RecordFault(source.PeerID)
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			if isQuarantined {
				return
			}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
