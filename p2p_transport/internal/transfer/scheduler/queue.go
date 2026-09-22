package scheduler

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueFull   = errors.New("queue is full")
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

// ChunkQueue is a thread-safe bounded ring buffer queue.
type ChunkQueue struct {
	buf      []ChunkTask
	head     int
	tail     int
	count    int
	capacity int
	closed   bool
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
}

// NewChunkQueue creates a new bounded ring buffer queue with the given capacity and initial tasks.
func NewChunkQueue(capacity int, tasks []ChunkTask) *ChunkQueue {
	if capacity <= 0 {
		capacity = len(tasks)
		if capacity < 16 {
			capacity = 16
		}
	}
	if len(tasks) > capacity {
		capacity = len(tasks)
	}

	q := &ChunkQueue{
		buf:      make([]ChunkTask, capacity),
		capacity: capacity,
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)

	for _, task := range tasks {
		q.buf[q.tail] = task
		q.tail = (q.tail + 1) % q.capacity
		q.count++
	}

	return q
}

// Pop retrieves and removes the next task from the queue.
// If the queue is empty, it blocks on condition variable signaling until a task is available,
// the context is canceled, or the queue is closed.
func (q *ChunkQueue) Pop(ctx context.Context) (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.count == 0 {
		if q.closed {
			return ChunkTask{}, false
		}
		if ctx != nil && ctx.Err() != nil {
			return ChunkTask{}, false
		}
		q.notEmpty.Wait()
	}

	if q.closed && q.count == 0 {
		return ChunkTask{}, false
	}

	task := q.buf[q.head]
	q.buf[q.head] = ChunkTask{} // Clear reference
	q.head = (q.head + 1) % q.capacity
	q.count--
	q.notFull.Signal()

	return task, true
}

// Next retrieves and removes the next task using context.Background().
func (q *ChunkQueue) Next() (ChunkTask, bool) {
	return q.Pop(context.Background())
}

// Push adds a task to the queue. If the queue is full, it blocks on condition variable signaling
// until capacity is available, the context is canceled, or the queue is closed.
func (q *ChunkQueue) Push(ctx context.Context, task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.count == q.capacity {
		if q.closed {
			return ErrQueueClosed
		}
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		q.notFull.Wait()
	}

	if q.closed {
		return ErrQueueClosed
	}

	q.buf[q.tail] = task
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	q.notEmpty.Signal()

	return nil
}

// TryPush attempts to push a task without blocking.
// Returns false if the queue is full or closed.
func (q *ChunkQueue) TryPush(task ChunkTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.count >= q.capacity {
		return false
	}

	q.buf[q.tail] = task
	q.tail = (q.tail + 1) % q.capacity
	q.count++
	q.notEmpty.Signal()

	return true
}

// Close closes the queue and unblocks all waiting readers and writers.
func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		q.notEmpty.Broadcast()
		q.notFull.Broadcast()
	}
}

// IsClosed returns whether the queue is closed.
func (q *ChunkQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.closed
}

// Len returns the current number of tasks in the queue.
func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.count
}

// Cap returns the maximum capacity of the queue.
func (q *ChunkQueue) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.capacity
}
