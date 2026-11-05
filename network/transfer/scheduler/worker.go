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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) bool {
	peerID := source.PeerID.String()

	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		task, found, hasTasks := queue.PopForPeer(peerID)
		if !found {
			if !hasTasks {
				select {
				case <-ctx.Done():
					return false
				case <-queue.NotifyChan():
					continue
				case <-time.After(20 * time.Millisecond):
					if queue.Len() == 0 {
						return false
					}
					continue
				}
			}

			// Peer has missed all currently available tasks in queue.
			// Yield immediately so worker slot can try another candidate source.
			return true
		}
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID, Duration: dur}:
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			select {
			case <-ctx.Done():
				return false
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID, Duration: dur}:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return false
		case results <- WorkerResult{Task: task, Error: nil, PeerID: peerID, BytesTransferred: int64(len(chunkData.Data)), Duration: dur}:
		}
	}
}
