package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

const (
	DefaultBaseBackoff     = 100 * time.Millisecond
	DefaultMaxQueueCapacity = 1000
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
	MaxQueueCap int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		BaseBackoff: DefaultBaseBackoff,
		MaxQueueCap: DefaultMaxQueueCapacity,
	}
}

func calculateBackoff(base time.Duration, attempts int) time.Duration {
	if attempts <= 1 {
		return base
	}
	shift := attempts - 1
	if shift > 30 {
		shift = 30
	}
	return base * time.Duration(1<<shift)
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queueCap := s.MaxQueueCap
	if queueCap <= 0 {
		queueCap = DefaultMaxQueueCapacity
	}
	if len(tasks) > queueCap {
		queueCap = len(tasks)
	}

	queue := NewChunkQueue(tasks, queueCap)
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
			case results <- WorkerResult{Error: fmt.Errorf("worker_done")}:
			case <-ctx.Done():
			}
		}(source, client)
	}

	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	pendingTasks := len(tasks)
	var pendingCompletions []WorkerResult

	baseBackoff := s.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = DefaultBaseBackoff
	}

	for (pendingTasks > 0 || len(pendingCompletions) > 0) && activeWorkers > 0 {
		var sendChan chan<- WorkerResult
		var head WorkerResult
		if len(pendingCompletions) > 0 {
			sendChan = completions
			head = pendingCompletions[0]
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case sendChan <- head:
			pendingCompletions = pendingCompletions[1:]

		case res := <-results:
			if res.Error != nil {
				if res.Error.Error() == "worker_done" {
					activeWorkers--
					continue
				}
				if strings.Contains(res.Error.Error(), "requeue failed") {
					return fmt.Errorf("chunk %x requeue error: %w", res.Task.ChunkID, res.Error)
				}

				// If provider returned ErrChunkNotFound, this is an expected candidate miss in a partial-replica CDN
				if errors.Is(res.Error, chunk.ErrRemoteChunkNotFound) {
					if res.Task.MissedPeers == nil {
						res.Task.MissedPeers = make(map[string]bool)
					}
					res.Task.MissedPeers[res.PeerID] = true

					if len(res.Task.MissedPeers) < len(sources) {
						if err := queue.Push(res.Task); err != nil {
							return fmt.Errorf("chunk %x requeue error: %w", res.Task.ChunkID, err)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					delay := calculateBackoff(baseBackoff, res.Task.Attempts)
					go func(t ChunkTask, d time.Duration) {
						timer := time.NewTimer(d)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							return
						case <-timer.C:
							if err := queue.Push(t); err != nil {
								select {
								case results <- WorkerResult{Task: t, Error: fmt.Errorf("requeue failed: %w", err)}:
								case <-ctx.Done():
								}
							}
						}
					}(res.Task, delay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				pendingCompletions = append(pendingCompletions, res)
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	// Flush remaining pending completions
	for len(pendingCompletions) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case completions <- pendingCompletions[0]:
			pendingCompletions = pendingCompletions[1:]
		}
	}

	return nil
}
