package scheduler

import (
	"context"
	"sync/atomic"
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

func SetTestThrottle(d time.Duration) {
	atomic.StoreInt64((*int64)(&TestThrottle), int64(d))
}

func getTestThrottle() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(&TestThrottle)))
}

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
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
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				return
			}
			continue
		}

		throttle := getTestThrottle()
		if throttle > 0 {
			select {
			case <-time.After(throttle):
			case <-ctx.Done():
				return
			}
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				return
			}
			continue
		}

		select {
		case results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}:
		case <-ctx.Done():
			return
		}
	}
}
