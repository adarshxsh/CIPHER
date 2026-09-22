package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

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
	Transport       *transport.Transport
	Engine          *engine.ContentEngine
	MaxAttempts     int
	MaxPeerFailures int

	mu           sync.RWMutex
	peerFailures map[peer.ID]int
	blacklisted  map[peer.ID]bool
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:       t,
		Engine:          eng,
		MaxAttempts:     maxAttempts,
		MaxPeerFailures: 1,
	}
}

func (s *Scheduler) GetPeerFailures(p peer.ID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.peerFailures == nil {
		return 0
	}
	return s.peerFailures[p]
}

func (s *Scheduler) IsBlacklisted(p peer.ID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.blacklisted == nil {
		return false
	}
	return s.blacklisted[p]
}

func (s *Scheduler) recordFailure(p peer.ID, maxFailures int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerFailures == nil {
		s.peerFailures = make(map[peer.ID]int)
	}
	if s.blacklisted == nil {
		s.blacklisted = make(map[peer.ID]bool)
	}
	s.peerFailures[p]++
	if s.peerFailures[p] >= maxFailures {
		s.blacklisted[p] = true
		return true
	}
	return s.blacklisted[p]
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	s.mu.Lock()
	s.peerFailures = make(map[peer.ID]int)
	s.blacklisted = make(map[peer.ID]bool)
	s.mu.Unlock()

	maxPeerFailures := s.MaxPeerFailures
	if maxPeerFailures <= 0 {
		maxPeerFailures = 1
	}

	queue := NewChunkQueue(tasks)
	defer queue.Close()

	results := make(chan WorkerResult, len(sources)*4)
	workerCancels := make(map[peer.ID]context.CancelFunc)

	// Start workers
	activeWorkers := 0
	for _, source := range sources {
		pID := source.PeerID
		if s.IsBlacklisted(pID) {
			continue
		}

		wCtx, cancel := context.WithCancel(ctx)
		workerCancels[pID] = cancel

		client, err := chunk.NewClient(wCtx, s.Transport, pID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", pID, err)
			s.recordFailure(pID, maxPeerFailures)
			cancel()
			continue
		}

		activeWorkers++
		go func(src Source, c *chunk.Client, wContext context.Context) {
			defer c.Close()
			runWorker(wContext, src, c, s.Engine, queue, results, s.IsBlacklisted)
			select {
			case results <- WorkerResult{Error: fmt.Errorf("worker_done"), PeerID: string(src.PeerID)}:
			case <-ctx.Done():
			}
		}(source, client, wCtx)
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

				// Track failure for this peer
				pID := peer.ID(res.PeerID)
				isBanned := s.recordFailure(pID, maxPeerFailures)
				if isBanned {
					if cancel, ok := workerCancels[pID]; ok {
						cancel()
					}
				}

				// Requeue logic
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					queue.Push(res.Task)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				select {
				case completions <- res:
				case <-ctx.Done():
					return ctx.Err()
				}
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
