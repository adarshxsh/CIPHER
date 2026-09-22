package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	Transport        *transport.Transport
	Engine           *engine.ContentEngine
	MaxAttempts      int
	BaseBackoff      time.Duration
	MaxBackoff       time.Duration
	MaxQueueCapacity int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:        t,
		Engine:           eng,
		MaxAttempts:      maxAttempts,
		BaseBackoff:      50 * time.Millisecond,
		MaxBackoff:       5 * time.Second,
		MaxQueueCapacity: 1000,
	}
}

func (s *Scheduler) calculateBackoff(attempts int) time.Duration {
	base := s.BaseBackoff
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	maxB := s.MaxBackoff
	if maxB <= 0 {
		maxB = 5 * time.Second
	}
	if attempts <= 0 {
		attempts = 1
	}

	shift := attempts - 1
	if shift > 30 {
		return maxB
	}
	multiplier := time.Duration(1 << shift)
	backoff := base * multiplier
	if backoff > maxB || backoff <= 0 {
		return maxB
	}
	return backoff
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if len(tasks) == 0 {
		return nil
	}

	queueCap := s.MaxQueueCapacity
	if queueCap <= 0 {
		queueCap = 1000
	}
	if queueCap < len(tasks) {
		queueCap = len(tasks)
	}

	queue := NewChunkQueueWithCapacity(tasks, queueCap)

	var wg sync.WaitGroup
	defer func() {
		queue.Close()
		wg.Wait()
	}()

	resultsCap := len(sources)*2 + len(tasks)
	if resultsCap < 100 {
		resultsCap = 100
	}
	results := make(chan WorkerResult, resultsCap)

	activeWorkers := 0
	for _, source := range sources {
		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		activeWorkers++
		wg.Add(1)
		go func(src Source, c *chunk.Client) {
			defer wg.Done()
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, results)
			select {
			case results <- WorkerResult{Error: fmt.Errorf("worker_done")}:
			case <-ctx.Done():
			}
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
						_ = queue.PushWithContext(ctx, res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					backoff := s.calculateBackoff(res.Task.Attempts)
					wg.Add(1)
					go func(task ChunkTask, delay time.Duration) {
						defer wg.Done()
						timer := time.NewTimer(delay)
						defer timer.Stop()

						select {
						case <-timer.C:
							_ = queue.PushWithContext(ctx, task)
						case <-ctx.Done():
							return
						}
					}(res.Task, backoff)
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
