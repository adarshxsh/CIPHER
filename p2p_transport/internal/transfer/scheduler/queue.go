package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cipher/internal/content/core"
)

const DefaultMaxQueueCapacity = 10000

var ErrQueueFull = errors.New("queue capacity exceeded")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	head     int
	tail     int
	size     int
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
	bufCap := capacity
	if len(tasks) > bufCap {
		bufCap = len(tasks)
	}

	q := &ChunkQueue{
		tasks:    make([]ChunkTask, bufCap),
		capacity: bufCap,
	}
	q.cond = sync.NewCond(&q.mu)

	for _, task := range tasks {
		if q.size >= q.capacity {
			break
		}
		q.tasks[q.tail] = task
		q.tail = (q.tail + 1) % q.capacity
		q.size++
	}

	return q
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return fmt.Errorf("queue is closed")
	}
	if q.size >= q.capacity {
		return ErrQueueFull
	}

	q.tasks[q.tail] = task
	q.tail = (q.tail + 1) % q.capacity
	q.size++
	q.cond.Signal()
	return nil
}

func (q *ChunkQueue) Next(ctx ...context.Context) (ChunkTask, bool) {
	var c context.Context
	if len(ctx) > 0 {
		c = ctx[0]
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == 0 && !q.closed {
		if c != nil && c.Err() != nil {
			return ChunkTask{}, false
		}
		q.cond.Wait()
	}

	if q.closed || q.size == 0 {
		return ChunkTask{}, false
	}
	if c != nil && c.Err() != nil {
		return ChunkTask{}, false
	}

	task := q.tasks[q.head]
	q.tasks[q.head] = ChunkTask{} // zero out for GC
	q.head = (q.head + 1) % q.capacity
	q.size--

	return task, true
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *ChunkQueue) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

