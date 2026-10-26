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

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
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
			case results <- WorkerResult{Task: task, Error: err, PeerID: peerID}:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return false
		case results <- WorkerResult{Task: task, Error: nil, PeerID: peerID}:
		}
	}
}
