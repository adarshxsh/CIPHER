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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, sourceQueue *SourceQueue, results chan<- WorkerResult) {
	peerIDStr := source.PeerID.String()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if queue.HasOnlyMissedFor(peerIDStr) {
			return
		}

		task, ok := queue.Next()
		if !ok {
			// Queue is temporarily empty. Wait briefly for re-queued tasks.
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			task, ok = queue.Next()
			if !ok {
				return
			}
		}

		// If this source already returned candidate miss for this task, requeue task and continue
		if task.MissedPeers != nil && task.MissedPeers[peerIDStr] {
			queue.Push(task)
			if queue.HasOnlyMissedFor(peerIDStr) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Millisecond):
			}
			continue
		}
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			if errors.Is(err, chunk.ErrRemoteChunkNotFound) {
				if queue.HasOnlyMissedFor(peerIDStr) {
					return
				}
				continue
			}
			return
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr, Duration: dur}
			return
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr, BytesTransferred: int64(len(chunkData.Data)), Duration: dur}
	}
}
