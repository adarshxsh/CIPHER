package scheduler

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var (
	ErrQueueOverflow = errors.New("queue capacity overflow: maximum capacity reached")
	ErrQueueClosed   = errors.New("queue is closed")
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	closed   bool
	notify   chan struct{}
	mu       sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask, capacity ...int) *ChunkQueue {
	cap := DefaultMaxQueueCapacity
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}
	if len(tasks) > cap {
		cap = len(tasks)
	}

	taskList := make([]ChunkTask, len(tasks))
	copy(taskList, tasks)

	return &ChunkQueue{
		tasks:    taskList,
		capacity: cap,
		notify:   make(chan struct{}),
	}
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}
	task := q.tasks[0]
	q.tasks[0] = ChunkTask{}
	q.tasks = q.tasks[1:]
	if len(q.tasks) == 0 {
		q.tasks = nil
	}
	return task, true
}

func (q *ChunkQueue) Pop(ctx context.Context) (ChunkTask, bool) {
	for {
		q.mu.Lock()
		if len(q.tasks) > 0 {
			task := q.tasks[0]
			q.tasks[0] = ChunkTask{}
			q.tasks = q.tasks[1:]
			if len(q.tasks) == 0 {
				q.tasks = nil
			}
			q.mu.Unlock()
			return task, true
		}
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}
		notify := q.notify
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		case <-notify:
		}
	}
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if q.capacity > 0 && len(q.tasks) >= q.capacity {
		return ErrQueueOverflow
	}
	q.tasks = append(q.tasks, task)
	close(q.notify)
	q.notify = make(chan struct{})
	return nil
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) Capacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

func (q *ChunkQueue) SetCapacity(capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.capacity = capacity
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.notify)
	}
}

