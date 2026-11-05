package scheduler

import (
	"context"
	"errors"
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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, tracker *PeerReputationTracker) {
	for {
		if tracker != nil && tracker.IsBlacklisted(source.PeerID) {
			return
		}

		if tracker != nil {
			backoff := tracker.GetBackoff(source.PeerID)
			if backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
		}

		if tracker != nil && tracker.IsBlacklisted(source.PeerID) {
			return
		}

		task, ok := queue.Next(ctx)
		if !ok {
			return // Queue empty, closed, or context canceled
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

		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if tracker != nil && !errors.Is(err, chunk.ErrRemoteChunkNotFound) {
				tracker.RecordFailure(source.PeerID)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if tracker != nil {
				tracker.RecordFailure(source.PeerID)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			continue
		}

		if tracker != nil {
			tracker.RecordSuccess(source.PeerID)
		}
		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String(), BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}
