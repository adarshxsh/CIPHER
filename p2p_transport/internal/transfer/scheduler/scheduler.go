package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	MaxQueueCap int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		MaxQueueCap: DefaultMaxQueueCapacity,
	}
}

func CalculateBackoff(attempt int, minBackoff, maxBackoff time.Duration) time.Duration {
	if minBackoff <= 0 {
		minBackoff = 100 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}
	if attempt <= 1 {
		return minBackoff
	}
	shift := uint(attempt - 1)
	if shift > 30 {
		return maxBackoff
	}
	backoff := minBackoff * time.Duration(1<<shift)
	if backoff > maxBackoff || backoff < 0 {
		return maxBackoff
	}
	return backoff
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queueCap := s.MaxQueueCap
	if queueCap <= 0 {
		queueCap = DefaultMaxQueueCapacity
	}
	queue := NewChunkQueueWithCapacity(tasks, queueCap)
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
			results <- WorkerResult{Error: fmt.Errorf("worker_done")} // Special signal
		}(source, client)
	}
	
	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}
	
	pendingTasks := len(tasks)
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
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
						queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < maxAttempts {
					delay := CalculateBackoff(res.Task.Attempts, s.MinBackoff, s.MaxBackoff)
					go func(task ChunkTask, backoff time.Duration) {
						select {
						case <-ctx.Done():
							return
						case <-time.After(backoff):
							if err := queue.Push(task); err != nil {
								select {
								case results <- WorkerResult{Task: task, Error: fmt.Errorf("requeue failed for chunk %x: %w", task.ChunkID, err)}:
								case <-ctx.Done():
								}
							}
						}
					}(res.Task, delay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, maxAttempts, res.Error)
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
