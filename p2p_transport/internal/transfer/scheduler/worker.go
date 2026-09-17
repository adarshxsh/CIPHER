package scheduler

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
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
	tracker *PeerTracker,
	trans *transport.Transport,
) {
	currentClient := client

	for {
		// 1. Check if peer is isolated / banned
		if tracker != nil && tracker.IsBanned(source.PeerID) {
			log.Printf("[Worker] Peer %s is banned/isolated. Stopping worker.", source.PeerID)
			if currentClient != nil {
				currentClient.Close()
			}
			return
		}

		// 2. Check exponential backoff delay for peers with negative scores
		if tracker != nil {
			if backoff := tracker.GetBackoff(source.PeerID); backoff > 0 {
				log.Printf("[Worker] Peer %s backoff delay: %v", source.PeerID, backoff)
				select {
				case <-ctx.Done():
					if currentClient != nil {
						currentClient.Close()
					}
					return
				case <-time.After(backoff):
				}
			}
		}

		if tracker != nil && tracker.IsBanned(source.PeerID) {
			if currentClient != nil {
				currentClient.Close()
			}
			return
		}

		// 3. Get next task from queue
		task, ok := queue.Next()
		if !ok {
			if currentClient != nil {
				currentClient.Close()
			}
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

		// 4. Ensure active client stream
		if currentClient == nil && trans != nil {
			var err error
			currentClient, err = chunk.NewClient(ctx, trans, source.PeerID, eng)
			if err != nil {
				if tracker != nil {
					tracker.RecordTimeout(source.PeerID)
				}
				results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
				continue
			}
		}

		if currentClient == nil {
			results <- WorkerResult{Task: task, Error: errors.New("no active client connection"), PeerID: source.PeerID.String()}
			continue
		}

		// 5. Fetch Chunk
		chunkData, err := currentClient.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			// Stream reset/closed in FetchChunk on error
			currentClient.Close()
			currentClient = nil

			if tracker != nil {
				if errors.Is(err, chunk.ErrChunkIntegrity) || strings.Contains(err.Error(), "corrupted") || strings.Contains(err.Error(), "mismatch") {
					tracker.RecordIntegrityFailure(source.PeerID)
				} else {
					tracker.RecordTimeout(source.PeerID)
				}
			}

			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		if TestThrottle > 0 {
			time.Sleep(TestThrottle)
		}

		// 6. Store chunk in Engine
		if err := eng.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: source.PeerID.String()}
			continue
		}

		// 7. Record success
		if tracker != nil {
			tracker.RecordSuccess(source.PeerID)
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: source.PeerID.String()}
	}
}
