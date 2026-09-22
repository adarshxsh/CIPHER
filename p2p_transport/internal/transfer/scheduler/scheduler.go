package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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
	Transport     *transport.Transport
	Engine        *engine.ContentEngine
	MaxAttempts   int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	QueueCapacity int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
	}
}

func computeBackoff(base, maxBackoff time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	exp := float64(uint64(1) << uint(shift))
	delayFloat := float64(base) * exp
	if delayFloat > float64(maxBackoff) {
		delayFloat = float64(maxBackoff)
	}
	delay := time.Duration(delayFloat)

	// Add jitter (up to 50% of delay)
	jitter := time.Duration(rand.Float64() * float64(delay) * 0.5)
	totalDelay := delay + jitter
	if totalDelay > maxBackoff {
		totalDelay = maxBackoff
	}
	return totalDelay
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	var queue *ChunkQueue
	if s.QueueCapacity > 0 {
		queue = NewChunkQueue(tasks, s.QueueCapacity)
	} else {
		queue = NewChunkQueue(tasks)
	}
	defer queue.Close()

	results := make(chan WorkerResult, len(sources)*2)

	dispatchCap := len(tasks)
	if dispatchCap < 1 {
		dispatchCap = 1
	}
	dispatchCh := make(chan WorkerResult, dispatchCap)
	dispatchDone := make(chan struct{})

	go func() {
		defer close(dispatchDone)
		for res := range dispatchCh {
			select {
			case completions <- res:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		close(dispatchCh)
		<-dispatchDone
	}()

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

	baseBackoff := s.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = 100 * time.Millisecond
	}
	maxBackoff := s.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}

	var wg sync.WaitGroup
	defer wg.Wait()

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
					backoff := computeBackoff(baseBackoff, maxBackoff, res.Task.Attempts)
					wg.Add(1)
					go func(t ChunkTask, delay time.Duration) {
						defer wg.Done()
						select {
						case <-ctx.Done():
							return
						case <-time.After(delay):
							queue.Push(t)
						}
					}(res.Task, backoff)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				dispatchCh <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
