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
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
	}
}

func (s *Scheduler) CalculateBackoff(attempts int) time.Duration {
	base := s.BaseBackoff
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxB := s.MaxBackoff
	if maxB <= 0 {
		maxB = 5 * time.Second
	}
	shift := attempts - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 30 {
		shift = 30
	}
	backoff := base * time.Duration(1<<shift)
	if backoff > maxB || backoff <= 0 {
		backoff = maxB
	}
	return backoff
}

type completionForwarder struct {
	ctx    context.Context
	out    chan<- WorkerResult
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []WorkerResult
	closed bool
	done   chan struct{}
}

func newCompletionForwarder(ctx context.Context, out chan<- WorkerResult) *completionForwarder {
	cf := &completionForwarder{
		ctx:  ctx,
		out:  out,
		done: make(chan struct{}),
	}
	cf.cond = sync.NewCond(&cf.mu)
	go cf.run()
	return cf
}

func (cf *completionForwarder) Push(res WorkerResult) {
	cf.mu.Lock()
	if !cf.closed {
		cf.buf = append(cf.buf, res)
		cf.cond.Signal()
	}
	cf.mu.Unlock()
}

func (cf *completionForwarder) Close() {
	cf.mu.Lock()
	if !cf.closed {
		cf.closed = true
		cf.cond.Broadcast()
	}
	cf.mu.Unlock()
	<-cf.done
}

func (cf *completionForwarder) run() {
	defer close(cf.done)
	for {
		cf.mu.Lock()
		for len(cf.buf) == 0 && !cf.closed {
			cf.cond.Wait()
		}
		if len(cf.buf) == 0 && cf.closed {
			cf.mu.Unlock()
			return
		}
		res := cf.buf[0]
		cf.buf[0] = WorkerResult{}
		cf.buf = cf.buf[1:]
		if len(cf.buf) == 0 {
			cf.buf = cf.buf[:0]
		}
		cf.mu.Unlock()

		if cf.out != nil {
			select {
			case cf.out <- res:
			case <-cf.ctx.Done():
				return
			}
		}
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)
	forwarder := newCompletionForwarder(ctx, completions)
	defer forwarder.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
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
		wg.Add(1)
		go func(src Source, c *chunk.Client) {
			defer wg.Done()
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
						_ = queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					backoff := s.CalculateBackoff(res.Task.Attempts)
					go func(task ChunkTask, delay time.Duration) {
						timer := time.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-timer.C:
							_ = queue.Push(task)
						case <-ctx.Done():
							return
						}
					}(res.Task, backoff)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				forwarder.Push(res)
				pendingTasks--
			}
		}
	}
	
	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}
	
	return nil
}
