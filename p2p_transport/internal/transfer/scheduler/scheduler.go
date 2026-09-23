package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	
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

const DefaultMaxConcurrency = 16

type Option func(*Scheduler)

func WithMaxConcurrency(max int) Option {
	return func(s *Scheduler) {
		if max > 0 {
			s.MaxConcurrency = max
		}
	}
}

type Scheduler struct {
	Transport      *transport.Transport
	Engine         *engine.ContentEngine
	MaxAttempts    int
	MaxConcurrency int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, opts ...Option) *Scheduler {
	s := &Scheduler{
		Transport:      t,
		Engine:         eng,
		MaxAttempts:    maxAttempts,
		MaxConcurrency: DefaultMaxConcurrency,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if len(tasks) == 0 {
		return nil
	}

	maxConcurrency := s.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = DefaultMaxConcurrency
	}

	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, maxConcurrency*2)

	sourceQueue := make([]Source, 0, len(sources))
	sourceMap := make(map[string]Source, len(sources))
	for _, src := range sources {
		sourceQueue = append(sourceQueue, src)
		sourceMap[src.PeerID.String()] = src
	}

	activeWorkers := 0

	dispatchNext := func() {
		for len(sourceQueue) > 0 && activeWorkers < maxConcurrency {
			if queue.Len() == 0 && activeWorkers > 0 {
				break
			}

			src := sourceQueue[0]
			sourceQueue = sourceQueue[1:]

			client, err := chunk.NewClient(ctx, s.Transport, src.PeerID, s.Engine)
			if err != nil {
				log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", src.PeerID, err)
				continue
			}

			activeWorkers++
			go func(source Source, c *chunk.Client) {
				defer c.Close()
				runWorker(ctx, source, c, s.Engine, queue, results)
				select {
				case results <- WorkerResult{PeerID: source.PeerID.String(), Error: fmt.Errorf("worker_done")}:
				case <-ctx.Done():
				}
			}(src, client)
		}
	}

	dispatchNext()

	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	pendingTasks := len(tasks)

	for pendingTasks > 0 && (activeWorkers > 0 || len(sourceQueue) > 0) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-results:
			if res.Error != nil {
				if res.Error.Error() == "worker_done" {
					activeWorkers--
					if src, ok := sourceMap[res.PeerID]; ok {
						sourceQueue = append(sourceQueue, src)
					}
					dispatchNext()
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
						dispatchNext()
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					queue.Push(res.Task)
					dispatchNext()
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				completions <- res
				pendingTasks--
				dispatchNext()
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
