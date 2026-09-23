package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
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
	Transport      *transport.Transport
	Engine         *engine.ContentEngine
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	QueueCapacity  int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:      t,
		Engine:         eng,
		MaxAttempts:    maxAttempts,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		BackoffFactor:  2.0,
		QueueCapacity:  1000,
	}
}

func (s *Scheduler) calculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	initial := s.InitialBackoff
	if initial <= 0 {
		initial = 50 * time.Millisecond
	}
	factor := s.BackoffFactor
	if factor <= 1.0 {
		factor = 2.0
	}
	maxB := s.MaxBackoff
	if maxB <= 0 {
		maxB = 2 * time.Second
	}

	delayFloat := float64(initial) * math.Pow(factor, float64(attempts-1))
	delay := time.Duration(delayFloat)
	if delay > maxB {
		delay = maxB
	}
	return delay
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	cap := s.QueueCapacity
	if cap <= 0 {
		cap = len(tasks) * 2
		if cap < 1024 {
			cap = 1024
		}
	}
	queue := NewChunkQueueWithCapacity(tasks, cap)
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
						if err := queue.Push(res.Task); err != nil {
							return fmt.Errorf("queue push failed on candidate miss: %w", err)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts and delay requeue with backoff
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					delay := s.calculateBackoff(res.Task.Attempts)
					if delay > 0 {
						taskToRequeue := res.Task
						go func() {
							timer := time.NewTimer(delay)
							defer timer.Stop()
							select {
							case <-ctx.Done():
								return
							case <-timer.C:
								_ = queue.Push(taskToRequeue)
							}
						}()
					} else {
						if err := queue.Push(res.Task); err != nil {
							return fmt.Errorf("queue push failed on retry: %w", err)
						}
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
