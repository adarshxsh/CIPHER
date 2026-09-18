package scheduler

import (
	"context"
	"log"
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

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker] Debug: worker cancelled: %v", ctx.Err())
			return ctx.Err()
		default:
		}

		task, ok := queue.Next()
		if !ok {
			return nil // Queue empty
		}
		
		if source.Available != nil {
			if _, has := source.Available[task.ChunkID]; !has {
				// We don't think this source has the chunk.
				// For now, we still try since discovery isn't fully robust.
			}
		}
		
		chunkData, err := client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				log.Printf("[Worker] Debug: worker cancelled during FetchChunk result send: %v", ctx.Err())
				return ctx.Err()
			}
			continue
		}

		if TestThrottle > 0 {
			select {
			case <-time.After(TestThrottle):
			case <-ctx.Done():
				log.Printf("[Worker] Debug: worker cancelled during throttle: %v", ctx.Err())
				return ctx.Err()
			}
		}

		if err := eng.PutChunk(ctx, chunkData); err != nil {
			select {
			case results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}:
			case <-ctx.Done():
				log.Printf("[Worker] Debug: worker cancelled during PutChunk result send: %v", ctx.Err())
				return ctx.Err()
			}
			continue
		}

		select {
		case results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}:
		case <-ctx.Done():
			log.Printf("[Worker] Debug: worker cancelled during success result send: %v", ctx.Err())
			return ctx.Err()
		}
	}
}
