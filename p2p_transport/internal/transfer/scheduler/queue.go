package scheduler

import (
	"context"
	"sync"

	"cipher/internal/content/core"
)

const (
	DefaultMaxQueueCapacity = 1000
	WorkerBufferMultiplier  = 2
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	ch       chan ChunkTask
	capacity int
	closed   bool
	mu       sync.RWMutex
	once     sync.Once
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return NewChunkQueueWithBounds(tasks, 1, DefaultMaxQueueCapacity)
}

func NewChunkQueueWithBounds(tasks []ChunkTask, workerCount int, maxLimit int) *ChunkQueue {
	if maxLimit <= 0 {
		maxLimit = DefaultMaxQueueCapacity
	}
	cap := len(tasks)
	minCap := workerCount * WorkerBufferMultiplier
	if minCap < 1 {
		minCap = 1
	}
	if cap < minCap {
		cap = minCap
	}
	if cap > maxLimit {
		cap = maxLimit
	}

	q := &ChunkQueue{
		ch:       make(chan ChunkTask, cap),
		capacity: cap,
	}

	for _, task := range tasks {
		select {
		case q.ch <- task:
		default:
			break
		}
	}

	return q
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	select {
	case task, ok := <-q.ch:
		return task, ok
	default:
		return ChunkTask{}, false
	}
}

func (q *ChunkQueue) Pop(ctx context.Context) (ChunkTask, bool) {
	select {
	case <-ctx.Done():
		return ChunkTask{}, false
	case task, ok := <-q.ch:
		return task, ok
	}
}

func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return false
	}
	q.mu.RUnlock()

	select {
	case q.ch <- task:
		return true
	default:
		return false
	}
}

func (q *ChunkQueue) PushCtx(ctx context.Context, task ChunkTask) bool {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return false
	}
	q.mu.RUnlock()

	select {
	case <-ctx.Done():
		return false
	case q.ch <- task:
		return true
	}
}

func (q *ChunkQueue) Len() int {
	return len(q.ch)
}

func (q *ChunkQueue) Cap() int {
	return q.capacity
}

func (q *ChunkQueue) Close() {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.ch)
		q.mu.Unlock()
	})
}

