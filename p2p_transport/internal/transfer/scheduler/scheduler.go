package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	
	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

const DefaultMaxWorkers = 16

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	MaxWorkers  int
}

type SchedulerOption func(*Scheduler)

func WithMaxWorkers(maxWorkers int) SchedulerOption {
	return func(s *Scheduler) {
		if maxWorkers > 0 {
			s.MaxWorkers = maxWorkers
		}
	}
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, opts ...SchedulerOption) *Scheduler {
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

type sourceEntry struct {
	source Source
	misses int
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if len(sources) == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	maxWorkers := s.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = DefaultMaxWorkers
	}

	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, maxWorkers*2)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	sourcesChan := make(chan sourceEntry, len(sources)*4)
	for _, src := range sources {
		sourcesChan <- sourceEntry{source: src, misses: 0}
	}

	numWorkers := len(sources)
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}

	activeWorkers := numWorkers
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				select {
				case results <- WorkerResult{Error: fmt.Errorf("worker_done")}:
				case <-ctx.Done():
				}
			}()

			for {
				select {
				case <-workerCtx.Done():
					return
				case entry, ok := <-sourcesChan:
					if !ok {
						return
					}

					client, err := chunk.NewClient(workerCtx, s.Transport, entry.source.PeerID, s.Engine)
					if err != nil {
						log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", entry.source.PeerID, err)
						continue
					}

					shouldRotate := runWorker(workerCtx, entry.source, client, s.Engine, queue, results)
					client.Close()

					if shouldRotate && queue.HasTaskForPeer(entry.source.PeerID.String()) {
						select {
						case <-workerCtx.Done():
							return
						case sourcesChan <- entry:
						default:
						}
					}
				}
			}
		}()
	}

	pendingTasks := len(tasks)

	for pendingTasks > 0 && activeWorkers > 0 {
		select {
		case <-ctx.Done():
			cancelWorkers()
			wg.Wait()
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
					cancelWorkers()
					wg.Wait()
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					queue.Push(res.Task)
				} else {
					cancelWorkers()
					wg.Wait()
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				completions <- res
				pendingTasks--
			}
		}
	}

	cancelWorkers()
	wg.Wait()

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
