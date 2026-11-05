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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, repMgr *ReputationManager) {
	peerIDStr := source.PeerID.String()
	for {
		if repMgr != nil && repMgr.IsExcluded(peerIDStr) {
			return
		}

		task, ok := queue.Next()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}

		if repMgr != nil && repMgr.IsExcluded(peerIDStr) {
			queue.Push(task)
			return
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
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr, BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}
