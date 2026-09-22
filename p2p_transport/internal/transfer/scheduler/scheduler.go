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
	Transport      *transport.Transport
	Engine         *engine.ContentEngine
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxCapacity    int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:      t,
		Engine:         eng,
		MaxAttempts:    maxAttempts,
		InitialBackoff: 50 * time.Millisecond,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	initialBackoff := s.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = 50 * time.Millisecond
	}

	queue := NewChunkQueue(tasks, s.MaxCapacity)
	defer queue.Close()

	compChan := make(chan WorkerResult, 100)
	compDone := make(chan struct{})

	go func() {
		defer close(compDone)
		var buffer []WorkerResult
		out := completions

		for {
			var current WorkerResult
			var activeOut chan<- WorkerResult

			if len(buffer) > 0 {
				current = buffer[0]
				activeOut = out
			}

			select {
			case res, ok := <-compChan:
				if !ok {
					if out != nil {
						for _, item := range buffer {
							select {
							case <-ctx.Done():
								return
							case out <- item:
							}
						}
					}
					return
				}
				buffer = append(buffer, res)
			case activeOut <- current:
				buffer = buffer[1:]
			case <-ctx.Done():
				return
			}
		}
	}()

	defer func() {
		close(compChan)
		<-compDone
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

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					backoff := initialBackoff * time.Duration(1<<(res.Task.Attempts-1))
					go func(task ChunkTask, delay time.Duration) {
						timer := time.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							return
						case <-timer.C:
							_ = queue.Push(ctx, task)
						}
					}(res.Task, backoff)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				compChan <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
