package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

var (
	BaseBackoff = 50 * time.Millisecond
	MaxBackoff  = 2 * time.Second
)

// CalculateBackoff calculates jittered exponential backoff delay based on attempt count.
// Base delay: 50ms, doubling up to 2s, plus up to 50% random jitter.
func CalculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	shift := uint(attempts - 1)
	if shift > 30 {
		shift = 30
	}
	delay := BaseBackoff * time.Duration(1<<shift)
	if delay > MaxBackoff || delay < 0 {
		delay = MaxBackoff
	}
	// Add jitter (up to 50% of delay)
	jitter := time.Duration(rand.Float64() * float64(delay) / 2)
	total := delay + jitter
	if total > MaxBackoff {
		total = MaxBackoff
	}
	return total
}

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	QueueCap    int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		QueueCap:    1000,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	capacity := s.QueueCap
	if capacity <= 0 {
		capacity = len(tasks)
	}
	queue := NewChunkQueue(capacity, tasks)
	defer queue.Close()

	// Ensure context cancellation immediately closes the queue to unblock workers
	stopCtxWatch := make(chan struct{})
	defer close(stopCtxWatch)
	go func() {
		select {
		case <-ctx.Done():
			queue.Close()
		case <-stopCtxWatch:
		}
	}()

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
						_ = queue.Push(ctx, res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Requeue logic with exponential backoff
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					backoffDelay := CalculateBackoff(res.Task.Attempts)
					go func(t ChunkTask, d time.Duration) {
						select {
						case <-ctx.Done():
							return
						case <-time.After(d):
							_ = queue.Push(ctx, t)
						}
					}(res.Task, backoffDelay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success: non-blocking dispatch to completions channel
				select {
				case completions <- res:
				default:
					go func(r WorkerResult) {
						select {
						case completions <- r:
						case <-ctx.Done():
						}
					}(res)
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
