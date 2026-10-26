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

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	queue := NewChunkQueue(tasks)
	results := make(chan WorkerResult, len(sources)*2)
	var wg sync.WaitGroup
	
	// Start workers
	activeWorkers := 0
	for _, source := range sources {
		client, err := chunk.NewClient(workerCtx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		activeWorkers++
		wg.Add(1)
		go func(src Source, c *chunk.Client) {
			defer wg.Done()
			defer c.Close()
			runWorker(workerCtx, src, c, s.Engine, queue, results)
			select {
			case results <- WorkerResult{Error: fmt.Errorf("worker_done")}:
			case <-workerCtx.Done():
			}
		}(source, client)
	}
	
	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	defer wg.Wait()
	
	pendingTasks := len(tasks)
	
	for pendingTasks > 0 && activeWorkers > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-results:
			if !ok {
				break
			}
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
				select {
				case completions <- res:
				case <-ctx.Done():
					return ctx.Err()
				}
				pendingTasks--
			}
		}
	}
	
	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}
	
	return nil
}
