package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport            *transport.Transport
	Engine               *engine.ContentEngine
	MaxAttempts          int
	MaxIntegrityFailures int
	MaxPeerFailures      int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:            t,
		Engine:               eng,
		MaxAttempts:          maxAttempts,
		MaxIntegrityFailures: 3,
		MaxPeerFailures:      5,
		InitialBackoff:       50 * time.Millisecond,
		MaxBackoff:           2 * time.Second,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	maxIntegrity := s.MaxIntegrityFailures
	if maxIntegrity <= 0 {
		maxIntegrity = 3
	}
	maxFailures := s.MaxPeerFailures
	if maxFailures <= 0 {
		maxFailures = 5
	}
	initBackoff := s.InitialBackoff
	if initBackoff <= 0 {
		initBackoff = 50 * time.Millisecond
	}
	maxBackoff := s.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}

	tracker := NewPeerTracker(maxIntegrity, maxFailures, initBackoff, maxBackoff)
	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*2+len(tasks)*4)

	activeWorkers := 0
	var wg sync.WaitGroup

	for _, source := range sources {
		activeWorkers++
		wg.Add(1)
		go func(src Source) {
			defer wg.Done()
			runWorkerLoop(ctx, s, src, nil, queue, tracker, results)
			results <- WorkerResult{Error: fmt.Errorf("worker_done"), PeerID: src.PeerID.String()}
		}(source)
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
