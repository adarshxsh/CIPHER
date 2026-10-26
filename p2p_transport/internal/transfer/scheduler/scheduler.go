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

type Config struct {
	MaxAttempts int
	Throttle    time.Duration
}

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	Config      Config
	MaxAttempts int
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int, cfg ...Config) *Scheduler {
	c := Config{MaxAttempts: maxAttempts}
	if len(cfg) > 0 {
		c = cfg[0]
		if c.MaxAttempts == 0 {
			c.MaxAttempts = maxAttempts
		}
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		Config:      c,
		MaxAttempts: c.MaxAttempts,
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
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
			runWorker(ctx, src, c, s.Engine, queue, results, s.Config)
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
