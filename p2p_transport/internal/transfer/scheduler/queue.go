package scheduler

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var ErrQueueClosed = errors.New("queue closed")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	ch        chan ChunkTask
	closed    chan struct{}
	closeOnce sync.Once
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	capacity := len(tasks)
	if capacity < 100 {
		capacity = 100
	}
	return NewChunkQueueWithCapacity(tasks, capacity)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity < len(tasks) {
		capacity = len(tasks)
	}
	if capacity < 1 {
		capacity = 1
	}

	q := &ChunkQueue{
		ch:     make(chan ChunkTask, capacity),
		closed: make(chan struct{}),
	}
	for _, t := range tasks {
		q.ch <- t
	}
	return q
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	return q.NextWithContext(context.Background())
}

func (q *ChunkQueue) NextWithContext(ctx context.Context) (ChunkTask, bool) {
	if err := ctx.Err(); err != nil {
		return ChunkTask{}, false
	}
	select {
	case task, ok := <-q.ch:
		return task, ok
	case <-q.closed:
		select {
		case task, ok := <-q.ch:
			return task, ok
		default:
			return ChunkTask{}, false
		}
	case <-ctx.Done():
		return ChunkTask{}, false
	}
}

func (q *ChunkQueue) Push(task ChunkTask) {
	_ = q.PushWithContext(context.Background(), task)
}

func (q *ChunkQueue) PushWithContext(ctx context.Context, task ChunkTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-q.closed:
		return ErrQueueClosed
	default:
	}

	select {
	case q.ch <- task:
		return nil
	case <-q.closed:
		return ErrQueueClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *ChunkQueue) Close() {
	q.closeOnce.Do(func() {
		close(q.closed)
	})
}

func (q *ChunkQueue) IsClosed() bool {
	select {
	case <-q.closed:
		return true
	default:
		return false
	}
}

func (q *ChunkQueue) Len() int {
	return len(q.ch)
}
