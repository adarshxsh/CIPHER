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
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	MaxQueueCap int

	queue *ChunkQueue
	mu    sync.Mutex
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		MaxQueueCap: DefaultMaxQueueCapacity,
	}
}

// CalculateBackoff computes exponential delay for task retry based on attempt count.
func (s *Scheduler) CalculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	base := s.BaseBackoff
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	shift := uint(attempts - 1)
	if shift > 30 {
		shift = 30
	}
	factor := 1 << shift
	delay := base * time.Duration(factor)
	if s.MaxBackoff > 0 && delay > s.MaxBackoff {
		delay = s.MaxBackoff
	}
	return delay
}

// Requeue applies exponential backoff delay and pushes task back onto the queue.
func (s *Scheduler) Requeue(ctx context.Context, task ChunkTask) error {
	s.mu.Lock()
	q := s.queue
	s.mu.Unlock()

	if q == nil {
		return fmt.Errorf("queue not initialized")
	}

	delay := s.CalculateBackoff(task.Attempts)
	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	if !q.Push(task) {
		return fmt.Errorf("requeue rejected task: %w", ErrQueueFull)
	}
	return nil
}

// QueueDepth returns the current depth metric of the task queue.
func (s *Scheduler) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queue == nil {
		return 0
	}
	return s.queue.Len()
}

// GetQueueDepth is an alias for QueueDepth to expose queue depth metric.
func (s *Scheduler) GetQueueDepth() int {
	return s.QueueDepth()
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	maxCap := s.MaxQueueCap
	if maxCap <= 0 {
		maxCap = DefaultMaxQueueCapacity
	}
	queue := NewChunkQueueWithCapacity(tasks, maxCap)

	s.mu.Lock()
	s.queue = queue
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.queue = nil
		s.mu.Unlock()
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
	if pendingTasks > maxCap {
		pendingTasks = maxCap
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
							return fmt.Errorf("requeue failed for missed chunk %x: %w", res.Task.ChunkID, ErrQueueFull)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					if err := s.Requeue(ctx, res.Task); err != nil {
						return fmt.Errorf("requeue failed for chunk %x: %w", res.Task.ChunkID, err)
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
