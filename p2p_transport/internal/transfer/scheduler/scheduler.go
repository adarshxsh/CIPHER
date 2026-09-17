package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
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

func calculateBackoff(attempt int, baseBackoff, maxBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := 1 << uint(min(attempt-1, 30))
	delay := baseBackoff * time.Duration(mult)
	if delay > maxBackoff || delay < 0 {
		delay = maxBackoff
	}

	jitter := time.Duration(rand.Float64() * float64(baseBackoff))
	total := delay + jitter
	if total > maxBackoff {
		total = maxBackoff
	}
	return total
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queueCap := s.QueueCapacity
	if queueCap <= 0 {
		queueCap = len(tasks) * 2
		if queueCap < 100 {
			queueCap = 100
		}
	}
	queue := NewChunkQueue(tasks, queueCap)
	defer queue.Close()

	results := make(chan WorkerResult, len(sources)*2)

	asyncCap := len(tasks)
	if asyncCap < 100 {
		asyncCap = 100
	}
	asyncCompletions := make(chan WorkerResult, asyncCap)
	var completionWg sync.WaitGroup
	completionWg.Add(1)
	go func() {
		defer completionWg.Done()
		for res := range asyncCompletions {
			select {
			case completions <- res:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		close(asyncCompletions)
		completionWg.Wait()
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
						if !queue.Push(res.Task) {
							log.Printf("[Scheduler] Warning: failed to requeue task %d, queue full", res.Task.Index)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					delay := calculateBackoff(res.Task.Attempts, baseBackoff, maxBackoff)
					go func(t ChunkTask, d time.Duration) {
						timer := time.NewTimer(d)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							return
						case <-timer.C:
							if !queue.Push(t) {
								log.Printf("[Scheduler] Warning: failed to requeue task %d, queue full", t.Index)
							}
						}
					}(res.Task, delay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				asyncCompletions <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}

