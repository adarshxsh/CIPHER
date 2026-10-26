package scheduler

import (
	"context"
	"fmt"
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
	tasks       []ChunkTask
	maxCapacity int
	slots       chan struct{}
	notify      chan struct{}
	mu          sync.Mutex
	closed      bool
}

func NewChunkQueue(tasks []ChunkTask, maxCapacity ...int) *ChunkQueue {
	cap := 100
	if len(maxCapacity) > 0 && maxCapacity[0] > 0 {
		cap = maxCapacity[0]
	} else if len(tasks) > cap {
		cap = len(tasks)
	}
	if cap < len(tasks) {
		cap = len(tasks)
	}

	q := &ChunkQueue{
		tasks:       make([]ChunkTask, 0, cap),
		maxCapacity: cap,
		slots:       make(chan struct{}, cap),
		notify:      make(chan struct{}, 1),
	}

	for _, task := range tasks {
		q.tasks = append(q.tasks, task)
		q.slots <- struct{}{}
	}

	if len(q.tasks) > 0 {
		q.notify <- struct{}{}
	}

	return q
}

func (q *ChunkQueue) Push(ctx context.Context, task ChunkTask) error {
	select {
	case q.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		<-q.slots
		return fmt.Errorf("queue is closed")
	}

	q.tasks = append(q.tasks, task)
	select {
	case q.notify <- struct{}{}:
	default:
	}

	return nil
}

func (q *ChunkQueue) Next(ctx context.Context) (ChunkTask, bool) {
	for {
		q.mu.Lock()
		if len(q.tasks) > 0 {
			task := q.tasks[0]
			q.tasks = q.tasks[1:]
			q.mu.Unlock()

			<-q.slots
			return task, true
		}

		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}
		q.mu.Unlock()

		if ctx == nil {
			ctx = context.Background()
		}

		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		case <-q.notify:
		}
	}
}

func (q *ChunkQueue) PopNonBlocking() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	<-q.slots
	return task, true
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		close(q.notify)
	}
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) Cap() int {
	return q.maxCapacity
}
