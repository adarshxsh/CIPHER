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
	Tracker     *reputation.Tracker
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, tracker ...*reputation.Tracker) *Scheduler {
	var tr *reputation.Tracker
	if len(tracker) > 0 && tracker[0] != nil {
		tr = tracker[0]
	} else {
		tr = reputation.NewTracker()
	}

	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		Tracker:     tr,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if s.Tracker == nil {
		s.Tracker = reputation.NewTracker()
	}

	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*4)

	// Start workers
	activeWorkers := 0
	for _, source := range sources {
		if s.Tracker.IsQuarantined(source.PeerID) {
			log.Printf("[Scheduler] Skipping quarantined source %s", source.PeerID)
			continue
		}

		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine, chunk.WithTracker(s.Tracker))
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		activeWorkers++
		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, s.Tracker, results)
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

					missedCount := len(res.Task.MissedPeers)
					failedCount := len(res.Task.FailedPeers)
					if missedCount+failedCount < len(sources) {
						queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, missedCount+failedCount, len(sources))
				}

				// Integrity / transport / protocol error
				if res.Task.FailedPeers == nil {
					res.Task.FailedPeers = make(map[string]bool)
				}
				res.Task.FailedPeers[res.PeerID] = true

				res.Task.Attempts++

				missedCount := len(res.Task.MissedPeers)
				failedCount := len(res.Task.FailedPeers)

				if res.Task.Attempts < s.MaxAttempts && (missedCount+failedCount < len(sources)) {
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
