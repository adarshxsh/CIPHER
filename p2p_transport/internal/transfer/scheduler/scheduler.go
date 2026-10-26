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

func calculateBackoff(attempts int) time.Duration {
	if attempts <= 1 {
		return 10 * time.Millisecond
	}
	shift := uint(attempts - 1)
	if shift > 10 {
		return 1 * time.Second
	}
	delay := 10 * time.Millisecond * (1 << shift)
	if delay > 1*time.Second {
		delay = 1 * time.Second
	}
	return delay
}

type completionBuffer struct {
	pushCh chan WorkerResult
	done   chan struct{}
}

func startCompletionBuffer(ctx context.Context, completions chan<- WorkerResult) *completionBuffer {
	buf := &completionBuffer{
		pushCh: make(chan WorkerResult, 1024),
		done:   make(chan struct{}),
	}

	go func() {
		defer close(buf.done)
		var queue []WorkerResult

		for {
			if len(queue) == 0 {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-buf.pushCh:
					if !ok {
						return
					}
					queue = append(queue, item)
				}
			} else {
				head := queue[0]
				select {
				case <-ctx.Done():
					return
				case item, ok := <-buf.pushCh:
					if !ok {
						for _, rem := range queue {
							select {
							case <-ctx.Done():
								return
							case completions <- rem:
							}
						}
						return
					}
					queue = append(queue, item)
				case completions <- head:
					queue = queue[1:]
				}
			}
		}
	}()

	return buf
}

func (b *completionBuffer) Push(res WorkerResult) {
	b.pushCh <- res
}

func (b *completionBuffer) Close(ctx context.Context) {
	close(b.pushCh)
	select {
	case <-ctx.Done():
	case <-b.done:
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	queue := NewChunkQueue(tasks)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Connect to sources and count active workers
	type connectedClient struct {
		source Source
		client *chunk.Client
	}
	var clients []connectedClient

	for _, source := range sources {
		client, err := chunk.NewClient(workerCtx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		clients = append(clients, connectedClient{source: source, client: client})
	}

	if len(clients) == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	resultsCap := len(clients) * 4
	if resultsCap < 16 {
		resultsCap = 16
	}
	results := make(chan WorkerResult, resultsCap)
	activeWorkers := len(clients)

	for _, cc := range clients {
		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(workerCtx, src, c, s.Engine, queue, results)
			sendResult(workerCtx, results, WorkerResult{Error: fmt.Errorf("worker_done")})
		}(cc.source, cc.client)
	}

	compBuf := startCompletionBuffer(workerCtx, completions)

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

				if errors.Is(res.Error, chunk.ErrRemoteChunkNotFound) {
					if res.Task.MissedPeers == nil {
						res.Task.MissedPeers = make(map[string]bool)
					}
					res.Task.MissedPeers[res.PeerID] = true

					if len(res.Task.MissedPeers) < len(sources) {
						pushTaskWithDefer(workerCtx, queue, res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					delay := calculateBackoff(res.Task.Attempts)
					go func(task ChunkTask, backoff time.Duration) {
						select {
						case <-workerCtx.Done():
							return
						case <-time.After(backoff):
							pushTaskWithDefer(workerCtx, queue, task)
						}
					}(res.Task, delay)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				compBuf.Push(res)
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	compBuf.Close(ctx)
	return nil
}
