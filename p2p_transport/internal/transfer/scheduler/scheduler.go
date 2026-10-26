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
	Transport    *transport.Transport
	Engine       *engine.ContentEngine
	MaxAttempts  int
	ScoreManager *reputation.ScoreManager
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return NewSchedulerWithScoreManager(t, eng, maxAttempts, nil)
}

func NewSchedulerWithScoreManager(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, sm *reputation.ScoreManager) *Scheduler {
	if sm == nil {
		sm = reputation.NewScoreManager(reputation.DefaultConfig())
	}
	return &Scheduler{
		Transport:    t,
		Engine:       eng,
		MaxAttempts:  maxAttempts,
		ScoreManager: sm,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*2)

	// Filter sources using ScoreManager
	var activeSources []Source
	for _, source := range sources {
		if s.ScoreManager != nil && s.ScoreManager.IsBanned(ctx, source.PeerID) {
			log.Printf("[Scheduler] Skipping banned source peer %s", source.PeerID)
			continue
		}
		activeSources = append(activeSources, source)
	}

	if len(activeSources) == 0 && len(sources) > 0 {
		return fmt.Errorf("all provided sources are banned due to reputation score")
	}

	// Start workers
	activeWorkers := 0
	for _, source := range activeSources {
		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			if s.ScoreManager != nil {
				s.ScoreManager.RecordError(ctx, source.PeerID, err)
			}
			continue
		}
		activeWorkers++
		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, results, s.ScoreManager)
			results <- WorkerResult{Error: fmt.Errorf("worker_done")} // Special signal
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
					queue.Push(res.Task)
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
