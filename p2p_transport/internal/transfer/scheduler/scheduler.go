package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport         *transport.Transport
	Engine            *engine.ContentEngine
	MaxAttempts       int
	ReputationManager *ReputationManager
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, rep ...*ReputationManager) *Scheduler {
	var rm *ReputationManager
	if len(rep) > 0 && rep[0] != nil {
		rm = rep[0]
	} else {
		rm = NewReputationManager()
	}
	return &Scheduler{
		Transport:         t,
		Engine:            eng,
		MaxAttempts:       maxAttempts,
		ReputationManager: rm,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if s.ReputationManager == nil {
		s.ReputationManager = NewReputationManager()
	}
	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*2)

	// Start workers
	activeWorkers := 0
	for _, source := range sources {
		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		activeWorkers++
		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, results, s.ReputationManager)
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
