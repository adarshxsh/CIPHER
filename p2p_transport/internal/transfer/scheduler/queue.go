package scheduler

import (
	"context"
	"sync"

	"cipher/internal/content/core"
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	items    []ChunkTask
	head     int
	tail     int
	count    int
	capacity int
	closed   bool
	mu       sync.Mutex
	notify   chan struct{}
}

// NewChunkQueue creates a new bounded task queue with optional capacity override.
// If capacity is not specified or less than len(tasks), it defaults to max(len(tasks), 16).
func NewChunkQueue(tasks []ChunkTask, opts ...int) *ChunkQueue {
	capVal := len(tasks)
	if len(opts) > 0 && opts[0] > 0 {
		capVal = opts[0]
	}
	if capVal < len(tasks) {
		capVal = len(tasks)
	}
	if capVal == 0 {
		capVal = 16
	}

	buf := make([]ChunkTask, capVal)
	copy(buf, tasks)

	tailVal := len(tasks)
	if capVal > 0 {
		tailVal = len(tasks) % capVal
	}

	return &ChunkQueue{
		items:    buf,
		head:     0,
		tail:     tailVal,
		count:    len(tasks),
		capacity: capVal,
		closed:   false,
		notify:   make(chan struct{}, 1),
	}
}

// Push adds a task to the queue if capacity permits. Returns true if queued, false if full or closed.
// Guarantees no slice reallocations as it operates on a fixed ring buffer.
func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.count >= q.capacity {
		return false
	}

	q.items[q.tail] = task
	q.tail = (q.tail + 1) % q.capacity
	q.count++

	q.signal()
	return true
}

// Next pops the next task from the queue. If the queue is empty and open, it blocks
// waiting for new tasks or queue closure until ctx is cancelled.
func (q *ChunkQueue) Next(ctx context.Context) (ChunkTask, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}
		if q.count > 0 {
			task := q.items[q.head]
			q.items[q.head] = ChunkTask{} // zero out to prevent memory retention
			q.head = (q.head + 1) % q.capacity
			q.count--

			if q.count > 0 {
				q.signal()
			}
			q.mu.Unlock()
			return task, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		case <-q.notify:
			// Signaled: loop back to lock and re-check count/closed
		}
	}
}

// TryNext pops the next task from the queue without blocking.
func (q *ChunkQueue) TryNext() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || q.count == 0 {
		return ChunkTask{}, false
	}

	task := q.items[q.head]
	q.items[q.head] = ChunkTask{}
	q.head = (q.head + 1) % q.capacity
	q.count--

	if q.count > 0 {
		q.signal()
	}
	return task, true
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

// Close closes the queue and unblocks all waiting goroutines.
func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		close(q.notify)
	}
}

func (q *ChunkQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
