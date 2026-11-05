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

func sendResult(ctx context.Context, results chan<- WorkerResult, res WorkerResult) bool {
	select {
	case <-ctx.Done():
		return false
	case results <- res:
		return true
	}
}

func pushTaskWithDefer(ctx context.Context, queue *ChunkQueue, task ChunkTask) bool {
	for {
		if queue.Push(task) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) {
	peerID := source.PeerID.String()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		task, ok := queue.NextForPeer(peerID)
		if !ok {
			task, ok = queue.Next()
			if !ok {
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}
		}

		if task.MissedPeers != nil && task.MissedPeers[peerID] {
			pushTaskWithDefer(ctx, queue, task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		start := time.Now()
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		dur := time.Since(start)
		if err != nil {
			if !sendResult(ctx, results, WorkerResult{Task: task, Error: err, PeerID: peerID, Duration: dur}) {
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			if !sendResult(ctx, results, WorkerResult{Task: task, Error: err, PeerID: peerID, Duration: dur}) {
				return
			}
			continue
		}

		if !sendResult(ctx, results, WorkerResult{Task: task, Error: nil, PeerID: peerID, BytesTransferred: int64(len(chunkData.Data)), Duration: dur}) {
			return
		}
	}
}
