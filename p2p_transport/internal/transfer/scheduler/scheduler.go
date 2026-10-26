package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
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
	Transport      *transport.Transport
	Engine         *engine.ContentEngine
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:      t,
		Engine:         eng,
		MaxAttempts:    maxAttempts,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		BackoffFactor:  2.0,
	}
}

func (s *Scheduler) calculateBackoff(attempts int) time.Duration {
	initBackoff := s.InitialBackoff
	if initBackoff <= 0 {
		initBackoff = 50 * time.Millisecond
	}
	maxBackoff := s.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}
	factor := s.BackoffFactor
	if factor <= 0 {
		factor = 2.0
	}

	if attempts < 1 {
		attempts = 1
	}

	temp := float64(initBackoff) * math.Pow(factor, float64(attempts-1))
	if temp > float64(maxBackoff) {
		temp = float64(maxBackoff)
	}

	// Add randomized jitter: Full Jitter [0, temp]
	jittered := rand.Float64() * temp
	if jittered < float64(time.Millisecond) && temp >= float64(time.Millisecond) {
		jittered = float64(time.Millisecond)
	}

	return time.Duration(jittered)
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
	defer queue.Close()

	results := make(chan WorkerResult, len(sources)*2+len(tasks))

	// Setup async completion dispatcher
	asyncCompletions := make(chan WorkerResult, len(tasks))
	dispatcherDone := make(chan struct{})

	go func() {
		defer close(dispatcherDone)
		for res := range asyncCompletions {
			if completions != nil {
				select {
				case completions <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	var closeOnce sync.Once
	closeAsync := func() {
		closeOnce.Do(func() {
			close(asyncCompletions)
		})
	}
	defer closeAsync()

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
						if !queue.Push(res.Task) {
							log.Printf("[Scheduler] Warning: Failed to requeue task %d (queue closed or full)", res.Task.Index)
						}
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					delay := s.calculateBackoff(res.Task.Attempts)
					go func(t ChunkTask, d time.Duration) {
						select {
						case <-ctx.Done():
							return
						case <-time.After(d):
							if !queue.Push(t) {
								log.Printf("[Scheduler] Warning: Failed to requeue task %d (queue closed or full)", t.Index)
							}
						}
					}(res.Task, delay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				asyncCompletions <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	// Close queue and flush async completions
	queue.Close()
	closeAsync()
	<-dispatcherDone

	return nil
}
