package scheduler

import (
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var (
	ErrQueueFull   = errors.New("queue capacity exceeded")
	ErrQueueClosed = errors.New("queue closed")
)

const DefaultMaxQueueCapacity = 10000

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
	mu       sync.Mutex
	cond     *sync.Cond
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return NewChunkQueueWithCapacity(tasks, DefaultMaxQueueCapacity)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity <= 0 {
		capacity = DefaultMaxQueueCapacity
	}
	if capacity < len(tasks) {
		capacity = len(tasks)
	}
	q := &ChunkQueue{
		tasks:    append([]ChunkTask(nil), tasks...),
		capacity: capacity,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.tasks) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}
	task := q.tasks[0]
	q.tasks[0] = ChunkTask{}
	q.tasks = q.tasks[1:]
	if len(q.tasks) == 0 {
		q.tasks = q.tasks[:0]
	}
	return task, true
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if q.capacity > 0 && len(q.tasks) >= q.capacity {
		return ErrQueueFull
	}
	q.tasks = append(q.tasks, task)
	q.cond.Signal()
	return nil
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) Depth() int {
	return q.Len()
}

func (q *ChunkQueue) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

func (q *ChunkQueue) SetCapacity(capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if capacity > 0 {
		q.capacity = capacity
	}
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
}
