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

const DefaultMaxWorkers = 16

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	MaxWorkers  int
}

type Option func(*Scheduler)

func WithMaxWorkers(maxWorkers int) Option {
	return func(s *Scheduler) {
		if maxWorkers > 0 {
			s.MaxWorkers = maxWorkers
		}
	}
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, opts ...Option) *Scheduler {
	s := &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		MaxWorkers:  DefaultMaxWorkers,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	maxWorkers := s.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = DefaultMaxWorkers
	}

	if len(sources) == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	queue := NewChunkQueue(tasks)
	sourceQueue := NewSourceQueue(sources)
	results := make(chan WorkerResult, maxWorkers*2)

	numWorkers := maxWorkers
	if len(sources) < numWorkers {
		numWorkers = len(sources)
	}

	activeWorkers := numWorkers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer func() {
				results <- WorkerResult{Error: fmt.Errorf("worker_done")}
			}()

			for {
				if ctx.Err() != nil {
					return
				}

				source, ok := sourceQueue.Pop()
				if !ok {
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Millisecond):
					}
					source, ok = sourceQueue.Pop()
					if !ok {
						if queue.Len() == 0 {
							return
						}
						// Wait once more
						select {
						case <-ctx.Done():
							return
						case <-time.After(10 * time.Millisecond):
						}
						source, ok = sourceQueue.Pop()
						if !ok && queue.Len() == 0 {
							return
						}
					}
				}

				client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine)
				if err != nil {
					log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
					continue
				}

				runWorker(ctx, source, client, s.Engine, queue, sourceQueue, results)
				client.Close()
			}
		}()
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
						queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					queue.Push(res.Task)
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
