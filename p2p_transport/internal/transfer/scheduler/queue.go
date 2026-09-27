package scheduler

import (
	"context"
	"sync"

	"cipher/internal/content/core"
)

const DefaultQueueCapacity = 1000

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	head     int
	capacity int
	closed   bool
	mu       sync.Mutex
	notFull  chan struct{}
	notEmpty chan struct{}
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	cap := DefaultQueueCapacity
	if len(tasks) > cap {
		cap = len(tasks)
	}
	return NewChunkQueueWithCapacity(tasks, cap)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity <= 0 {
		capacity = DefaultQueueCapacity
	}
	if len(tasks) > capacity {
		capacity = len(tasks)
	}

	taskList := make([]ChunkTask, len(tasks), capacity)
	copy(taskList, tasks)

	return &ChunkQueue{
		tasks:    taskList,
		head:     0,
		capacity: capacity,
		notFull:  make(chan struct{}, 1),
		notEmpty: make(chan struct{}, 1),
	}
}

func (q *ChunkQueue) signalNotEmpty() {
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
}

func (q *ChunkQueue) signalNotFull() {
	select {
	case q.notFull <- struct{}{}:
	default:
	}
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks) - q.head
}

func (q *ChunkQueue) Capacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	q.signalNotEmpty()
	q.signalNotFull()
}

func (q *ChunkQueue) Push(task ChunkTask) {
	_ = q.PushCtx(context.Background(), task)
}

func (q *ChunkQueue) PushCtx(ctx context.Context, task ChunkTask) error {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return context.Canceled
		}

		currentLen := len(q.tasks) - q.head
		if currentLen < q.capacity {
			if q.head > 128 && q.head > len(q.tasks)/2 {
				copy(q.tasks, q.tasks[q.head:])
				q.tasks = q.tasks[:currentLen]
				q.head = 0
			}

			q.tasks = append(q.tasks, task)
			q.mu.Unlock()

			q.signalNotEmpty()
			return nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.notFull:
		}
	}
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	task, ok, _ := q.popInternal(context.Background(), false)
	return task, ok
}

func (q *ChunkQueue) NextCtx(ctx context.Context) (ChunkTask, bool) {
	task, ok, _ := q.popInternal(ctx, true)
	return task, ok
}

func (q *ChunkQueue) popInternal(ctx context.Context, waitOnEmpty bool) (ChunkTask, bool, error) {
	for {
		q.mu.Lock()
		currentLen := len(q.tasks) - q.head
		if currentLen > 0 {
			task := q.tasks[q.head]
			q.tasks[q.head] = ChunkTask{}
			q.head++

			if q.head >= len(q.tasks) {
				q.tasks = q.tasks[:0]
				q.head = 0
			}

			q.mu.Unlock()

			q.signalNotFull()
			return task, true, nil
		}

		if q.closed || !waitOnEmpty {
			q.mu.Unlock()
			return ChunkTask{}, false, nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ChunkTask{}, false, ctx.Err()
		case <-q.notEmpty:
		}
	}
}
