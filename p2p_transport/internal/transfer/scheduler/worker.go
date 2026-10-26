package scheduler

import (
	"context"
	"time"

	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
)

// WorkerResult is the result of a worker attempting a chunk
type WorkerResult struct {
	Task   ChunkTask
	Error  error
	PeerID string // To track contribution
}

// WorkerConfig defines parameters for scheduler worker instances.
type WorkerConfig struct {
	Throttle time.Duration
}

// Worker represents a transfer scheduler worker instance.
type Worker struct {
	Source Source
	Client *chunk.Client
	Engine *engine.ContentEngine
	Queue  *ChunkQueue
	Config WorkerConfig
}

// NewWorker constructs a new worker instance.
func NewWorker(source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, cfg WorkerConfig) *Worker {
	return &Worker{
		Source: source,
		Client: client,
		Engine: eng,
		Queue:  queue,
		Config: cfg,
	}
}

// Run executes task retrieval and processing for the worker.
func (w *Worker) Run(ctx context.Context, results chan<- WorkerResult) {
	for {
		task, ok := w.Queue.Next()
		if !ok {
			return // Queue empty
		}
		
		// If this source already returned candidate miss for this task, requeue and yield
		if task.MissedPeers != nil && task.MissedPeers[w.Source.PeerID.String()] {
			w.Queue.Push(task)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		
		chunkData, err := w.Client.FetchChunk(ctx, task.ChunkID)
		if err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: w.Source.PeerID.String()}
			continue
		}

		if w.Config.Throttle > 0 {
			time.Sleep(w.Config.Throttle)
		}

		if err := w.Engine.PutChunk(ctx, chunkData); err != nil {
			results <- WorkerResult{Task: task, Error: err, PeerID: w.Source.PeerID.String()}
			continue
		}

		results <- WorkerResult{Task: task, Error: nil, PeerID: w.Source.PeerID.String()}
	}
}

func runWorker(ctx context.Context, source Source, client *chunk.Client, eng *engine.ContentEngine, queue *ChunkQueue, results chan<- WorkerResult, cfg WorkerConfig) {
	w := NewWorker(source, client, eng, queue, cfg)
	w.Run(ctx, results)
}
