package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

const (
	DefaultBaseBackoff = 100 * time.Millisecond
	DefaultMaxBackoff  = 5 * time.Second
)

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport     *transport.Transport
	Engine        *engine.ContentEngine
	MaxAttempts   int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	DisableJitter bool
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		BaseBackoff: DefaultBaseBackoff,
		MaxBackoff:  DefaultMaxBackoff,
	}
}

func (s *Scheduler) CalculateBackoff(attempts int) time.Duration {
	base := s.BaseBackoff
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	max := s.MaxBackoff
	if max <= 0 {
		max = DefaultMaxBackoff
	}
	return CalculateBackoffWithBase(attempts, base, max, s.DisableJitter)
}

func CalculateBackoffWithBase(attempts int, base, max time.Duration, disableJitter bool) time.Duration {
	if attempts <= 0 {
		attempts = 1
	}
	exp := attempts - 1
	if exp > 30 {
		exp = 30
	}

	backoff := base * (1 << exp)
	if backoff > max || backoff <= 0 {
		backoff = max
	}

	if disableJitter {
		return backoff
	}

	// Add randomized jitter (0% to 20% of backoff)
	jitter := time.Duration(rand.Float64() * float64(backoff) * 0.2)
	total := backoff + jitter
	if total > max {
		total = max
	}
	return total
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
	defer queue.Close()

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
			runWorker(ctx, src, c, s.Engine, queue, results)
			select {
			case <-ctx.Done():
			case results <- WorkerResult{Error: fmt.Errorf("worker_done")}:
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
						res.Task.ReadyAt = time.Time{}
						if err := queue.Push(res.Task); err != nil {
							return fmt.Errorf("failed to requeue task: %w", err)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					backoff := s.CalculateBackoff(res.Task.Attempts)
					res.Task.ReadyAt = time.Now().Add(backoff)
					if err := queue.Push(res.Task); err != nil {
						return fmt.Errorf("failed to requeue task: %w", err)
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
