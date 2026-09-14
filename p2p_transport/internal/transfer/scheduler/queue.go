package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"cipher/internal/content/core"
)

var ErrQueueFull = errors.New("queue capacity exceeded")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type delayedTask struct {
	task    ChunkTask
	readyAt time.Time
}

type ChunkQueue struct {
	buffer   []ChunkTask
	capacity int
	head     int
	tail     int
	count    int
	delayed  []delayedTask
	closed   bool
	mu       sync.Mutex
	notEmpty *sync.Cond
}

func NewChunkQueue(args ...interface{}) *ChunkQueue {
	var capacity int
	var initialTasks []ChunkTask

	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			capacity = v
		case []ChunkTask:
			initialTasks = v
		case ChunkTask:
			initialTasks = append(initialTasks, v)
		}
	}

	if capacity < len(initialTasks) {
		capacity = len(initialTasks)
	}
	if capacity == 0 {
		capacity = 1
	}

	q := &ChunkQueue{
		buffer:   make([]ChunkTask, capacity),
		capacity: capacity,
		delayed:  make([]delayedTask, 0, capacity),
	}
	q.notEmpty = sync.NewCond(&q.mu)

	for _, task := range initialTasks {
		_ = q.pushLocked(task, 0)
	}

	return q
}

func (q *ChunkQueue) pushLocked(task ChunkTask, delay time.Duration) error {
	if q.closed {
		return errors.New("queue closed")
	}

	if q.capacity == 0 || (q.count+len(q.delayed)) >= q.capacity {
		return ErrQueueFull
	}

	if delay <= 0 {
		q.buffer[q.tail] = task
		q.tail = (q.tail + 1) % q.capacity
		q.count++
		q.notEmpty.Broadcast()
		return nil
	}

	readyAt := time.Now().Add(delay)
	dt := delayedTask{task: task, readyAt: readyAt}

	idx := len(q.delayed)
	for i, existing := range q.delayed {
		if readyAt.Before(existing.readyAt) {
			idx = i
			break
		}
	}

	q.delayed = append(q.delayed, delayedTask{})
	copy(q.delayed[idx+1:], q.delayed[idx:])
	q.delayed[idx] = dt
	q.notEmpty.Broadcast()

	return nil
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pushLocked(task, 0)
}

func (q *ChunkQueue) PushWithBackoff(task ChunkTask, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pushLocked(task, delay)
}

func (q *ChunkQueue) PushWithContext(ctx context.Context, task ChunkTask) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return q.Push(task)
}

func (q *ChunkQueue) processDelayedLocked() {
	now := time.Now()
	for len(q.delayed) > 0 {
		if now.Before(q.delayed[0].readyAt) {
			break
		}
		readyTask := q.delayed[0].task
		copy(q.delayed, q.delayed[1:])
		q.delayed = q.delayed[:len(q.delayed)-1]

		if q.count < q.capacity {
			q.buffer[q.tail] = readyTask
			q.tail = (q.tail + 1) % q.capacity
			q.count++
		}
	}
}

func (q *ChunkQueue) NextWithContext(ctx context.Context) (ChunkTask, bool) {
	if ctx != nil && ctx.Err() != nil {
		return ChunkTask{}, false
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if ctx != nil && ctx.Err() != nil {
			return ChunkTask{}, false
		}
		if q.closed {
			return ChunkTask{}, false
		}

		q.processDelayedLocked()

		if q.count > 0 {
			task := q.buffer[q.head]
			var zero ChunkTask
			q.buffer[q.head] = zero
			q.head = (q.head + 1) % q.capacity
			q.count--
			return task, true
		}

		if len(q.delayed) == 0 {
			return ChunkTask{}, false
		}

		earliest := q.delayed[0].readyAt
		waitDuration := time.Until(earliest)
		if waitDuration <= 0 {
			continue
		}

		q.mu.Unlock()

		timer := time.NewTimer(waitDuration)
		var ctxDone <-chan struct{}
		if ctx != nil {
			ctxDone = ctx.Done()
		}

		select {
		case <-timer.C:
		case <-ctxDone:
			timer.Stop()
			q.mu.Lock()
			return ChunkTask{}, false
		}

		timer.Stop()
		q.mu.Lock()
	}
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	return q.NextWithContext(context.Background())
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count + len(q.delayed)
}

func (q *ChunkQueue) ActiveLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

func (q *ChunkQueue) DelayedLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.delayed)
}

func (q *ChunkQueue) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notEmpty.Broadcast()
}
