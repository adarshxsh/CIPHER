package scheduler

import (
	"context"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task   ChunkTask
	Error  error
	PeerID string // To track contribution
}

var TestThrottle time.Duration

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, tracker *reputation.Tracker, results chan<- WorkerResult) {
	peerIDStr := source.PeerID.String()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if tracker != nil && tracker.IsQuarantined(source.PeerID) {
			if client != nil {
				client.Close()
			}
			return
		}

		task, ok, shouldExit := queue.PopForPeer(source.PeerID, tracker)
		if shouldExit {
			if client != nil && tracker != nil && tracker.IsQuarantined(source.PeerID) {
				client.Close()
			}
			return
		}

		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}

		// Double check quarantine before requesting chunk
		if tracker != nil && tracker.IsQuarantined(source.PeerID) {
			if client != nil {
				client.Close()
			}
			queue.Push(task)
			queue.FinishTask(task)
			return
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			queue.FinishTask(task)
			if tracker != nil && tracker.IsQuarantined(source.PeerID) {
				if client != nil {
					client.Close()
				}
			}
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr}
			if tracker != nil && tracker.IsQuarantined(source.PeerID) {
				return
			}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			queue.FinishTask(task)
			results <- WorkerResult{Task: task, Error: err, PeerID: peerIDStr}
			continue
		}

		queue.FinishTask(task)
		results <- WorkerResult{Task: task, Error: nil, PeerID: peerIDStr}
	}
}
