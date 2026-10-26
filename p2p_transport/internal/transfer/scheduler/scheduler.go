package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	Tracker     *reputation.PeerReputationTracker
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
	}
}

func (s *Scheduler) hasActiveNonBannedWorkers(activeWorkerPeers map[peer.ID]int) bool {
	for pID, count := range activeWorkerPeers {
		if count > 0 {
			if s.Tracker == nil || !s.Tracker.IsBanned(pID) {
				return true
			}
		}
	}
	return false
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*2)

	activeWorkerPeers := make(map[peer.ID]int)
	activeWorkers := 0

	// Start workers
	for _, source := range sources {
		if s.Tracker != nil && s.Tracker.IsBanned(source.PeerID) {
			log.Printf("[Scheduler] Skipping banned source peer %s", source.PeerID)
			continue
		}

		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine, chunk.WithTracker(s.Tracker))
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			if s.Tracker != nil {
				s.Tracker.RecordConnectionFailure(source.PeerID)
			}
			continue
		}
		activeWorkers++
		activeWorkerPeers[source.PeerID]++

		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, results, s.Tracker)
			results <- WorkerResult{Error: fmt.Errorf("worker_done"), PeerID: src.PeerID.String()} // Special signal
		}(source, client)
	}

	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	pendingTasks := len(tasks)

	for pendingTasks > 0 && activeWorkers > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-results:
			if res.Error != nil {
				if res.Error.Error() == "worker_done" {
					var pID peer.ID
					if decoded, err := peer.Decode(res.PeerID); err == nil {
						pID = decoded
					} else {
						pID = peer.ID(res.PeerID)
					}
					if activeWorkerPeers[pID] > 0 {
						activeWorkerPeers[pID]--
					}
					activeWorkers--
					continue
				}
				// If provider returned ErrChunkNotFound, this is an expected candidate miss in a partial-replica CDN
				if errors.Is(res.Error, chunk.ErrRemoteChunkNotFound) {
					if res.Task.MissedPeers == nil {
						res.Task.MissedPeers = make(map[string]bool)
					}
					res.Task.MissedPeers[res.PeerID] = true

					if len(res.Task.MissedPeers) < len(sources) {
						queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					if s.hasActiveNonBannedWorkers(activeWorkerPeers) {
						queue.Push(res.Task)
					} else {
						return fmt.Errorf("chunk %x failed and no non-banned active workers remain: %w", res.Task.ChunkID, res.Error)
					}
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				completions <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
