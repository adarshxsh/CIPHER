package scheduler

import (
	"context"
	"time"

	"cipher/network/content/engine"
	"cipher/network/protocol/chunk"
	"cipher/network/reputation"
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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, rep *reputation.PeerReputationManager) {
	for {
		if rep != nil && rep.IsBanned(source.PeerID) {
			return
		}

		task, ok := queue.Next()
		if !ok {
			return // Queue empty
		}

		if rep != nil && rep.IsBanned(source.PeerID) {
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
		
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if rep != nil && client.ReputationManager() == nil {
				rep.RecordFailure(source.PeerID)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if rep != nil {
				rep.RecordFailure(source.PeerID)
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String(), Duration: dur}
			continue
		}

		if rep != nil {
			rep.RecordSuccess(source.PeerID)
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String(), BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}
