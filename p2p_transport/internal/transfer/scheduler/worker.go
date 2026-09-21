package scheduler

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

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

func runWorker(
	ctx context.Context,
	source Source,
	client *chunk.Client,
	eng *engine.ContentEngine,
	queue *ChunkQueue,
	results chan<- WorkerResult,
	isBlacklisted func(p peer.ID) bool,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if isBlacklisted != nil && isBlacklisted(source.PeerID) {
			return
		}

		task, ok := queue.Next(ctx)
		if !ok {
			return // Queue empty or closed
		}

		if isBlacklisted != nil && isBlacklisted(source.PeerID) {
			queue.Push(task)
			return
		}

		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[string(source.PeerID)] {
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
				queue.Push(task)
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
		}

		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: string(source.PeerID)}:
			case <-ctx.Done():
			}
			return
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: string(source.PeerID)}:
			case <-ctx.Done():
			}
			return
		}

		select {
		case results <- WorkerResult{Task: task, Error: nil, PeerID: string(source.PeerID)}:
		case <-ctx.Done():
		}
	}
}
