package scheduler

import (
	"context"
	"math"
	"sync"
	"time"

	"cipher/internal/content/core"
)

type ChunkTask struct {
	Index         int
	ChunkID       core.ChunkID
	Attempts      int
	MissedPeers   map[string]bool
	NextAvailable time.Time
}

// ChunkQueue is a thread-safe bounded ring buffer task queue.
// It maintains O(1) memory by avoiding dynamic slice allocations during task pops and pushes.
type ChunkQueue struct {
	buf      []ChunkTask
	head     int
	tail     int
	count    int
	capacity int
	waiters  int
	closed   bool
	mu       sync.Mutex
	cond     *sync.Cond
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	cap := len(tasks)
	if cap < 16 {
		cap = 16
	}

	q := &ChunkQueue{
		buf:      make([]ChunkTask, cap),
		capacity: cap,
	}
	q.cond = sync.NewCond(&q.mu)

	for _, task := range tasks {
		q.buf[q.tail] = task
		q.tail = (q.tail + 1) % q.capacity
		q.count++
	}

	return q
}

// Next pops the next available task from the ring buffer queue.
// Performs 0 heap allocations in steady state.
func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed && q.count == 0 {
		return ChunkTask{}, false
	}
	if q.count == 0 {
		return ChunkTask{}, false
	}

	now := time.Now()
	readyIndex := -1

	// Scan ring buffer from head to tail to find a ready task
	for i := 0; i < q.count; i++ {
		idx := (q.head + i) % q.capacity
		task := q.buf[idx]

		if task.NextAvailable.IsZero() || !task.NextAvailable.After(now) {
			readyIndex = idx
			break
		}
	}

	if readyIndex != -1 {
		task := q.buf[readyIndex]

		if readyIndex == q.head {
			q.buf[q.head] = ChunkTask{} // zero slot
			q.head = (q.head + 1) % q.capacity
		} else {
			q.buf[readyIndex] = q.buf[q.head]
			q.buf[q.head] = ChunkTask{}
			q.head = (q.head + 1) % q.capacity
		}

		q.count--
		return task, true
	}

	return ChunkTask{}, false
}

// NextWithContext pops the next available task from the ring buffer queue with context cancellation and backoff delay support.
func (q *ChunkQueue) NextWithContext(ctx context.Context) (ChunkTask, bool) {
	if ctx == nil {
		return q.Next()
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.closed && q.count == 0 {
			return ChunkTask{}, false
		}

		if ctx.Err() != nil {
			return ChunkTask{}, false
		}

		now := time.Now()
		readyIndex := -1
		var minWait time.Duration = -1

		if q.count > 0 {
			// Scan ring buffer from head to tail to find a ready task
			for i := 0; i < q.count; i++ {
				idx := (q.head + i) % q.capacity
				task := q.buf[idx]

				if task.NextAvailable.IsZero() || !task.NextAvailable.After(now) {
					readyIndex = idx
					break
				}

				wait := task.NextAvailable.Sub(now)
				if minWait == -1 || wait < minWait {
					minWait = wait
				}
			}
		}

		if readyIndex != -1 {
			task := q.buf[readyIndex]

			if readyIndex == q.head {
				q.buf[q.head] = ChunkTask{} // zero slot to prevent memory leaks
				q.head = (q.head + 1) % q.capacity
			} else {
				q.buf[readyIndex] = q.buf[q.head]
				q.buf[q.head] = ChunkTask{}
				q.head = (q.head + 1) % q.capacity
			}

			q.count--
			return task, true
		}

		// Need to wait for new push, close, context done, or backoff timeout
		q.waiters++

		var stopCtxWatch func()
		if ctx.Done() != nil {
			doneCh := ctx.Done()
			stopCh := make(chan struct{})
			stopCtxWatch = func() { close(stopCh) }
			go func() {
				select {
				case <-doneCh:
					q.mu.Lock()
					q.cond.Broadcast()
					q.mu.Unlock()
				case <-stopCh:
				}
			}()
		}

		if minWait > 0 {
			timer := time.AfterFunc(minWait, func() {
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			})
			q.cond.Wait()
			timer.Stop()
		} else {
			q.cond.Wait()
		}

		if stopCtxWatch != nil {
			stopCtxWatch()
		}
		q.waiters--
	}
}

// Push appends a task back to the ring buffer queue without slice allocation in steady state.
func (q *ChunkQueue) Push(task ChunkTask) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	if q.count == q.capacity {
		// Ring buffer capacity expansion if queue overflows initial bounds
		newCap := q.capacity * 2
		newBuf := make([]ChunkTask, newCap)
		for i := 0; i < q.count; i++ {
			newBuf[i] = q.buf[(q.head + i)%q.capacity]
		}
		q.buf = newBuf
		q.head = 0
		q.tail = q.count
		q.capacity = newCap
	}

	q.buf[q.tail] = task
	q.tail = (q.tail + 1) % q.capacity
	q.count++

	if q.waiters > 0 {
		q.cond.Broadcast()
	}
}

// Close marks the queue as closed and notifies all waiting consumers.
func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
}

// Len returns the current number of tasks in the queue.
func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// CalculateBackoff computes exponential backoff delay for a given attempt count.
func CalculateBackoff(attempt int, baseDelay time.Duration) time.Duration {
	if attempt <= 0 {
		return 0
	}
	if baseDelay <= 0 {
		baseDelay = 50 * time.Millisecond
	}
	factor := math.Pow(2, float64(attempt-1))
	return time.Duration(float64(baseDelay) * factor)
}

