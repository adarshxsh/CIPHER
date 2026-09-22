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
	tasks      []ChunkTask
	capacity   int
	closed     bool
	closedChan chan struct{}
	notify     chan struct{}
	mu         sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask, capacity ...int) *ChunkQueue {
	capVal := 0
	if len(capacity) > 0 {
		capVal = capacity[0]
	}
	if capVal <= 0 {
		if len(tasks) > 0 {
			capVal = len(tasks) * 2
		} else {
			capVal = 100
		}
	}

	notifyCap := capVal + len(tasks) + 100
	q := &ChunkQueue{
		tasks:      make([]ChunkTask, 0, len(tasks)),
		capacity:   capVal,
		closedChan: make(chan struct{}),
		notify:     make(chan struct{}, notifyCap),
	}
	for _, t := range tasks {
		q.tasks = append(q.tasks, t)
		q.notify <- struct{}{}
	}
	return q
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task, true
}

func (q *ChunkQueue) NextWithContext(ctx context.Context) (ChunkTask, bool) {
	for {
		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		default:
		}

		q.mu.Lock()
		if len(q.tasks) > 0 {
			task := q.tasks[0]
			q.tasks = q.tasks[1:]
			q.mu.Unlock()
			return task, true
		}
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		case <-q.closedChan:
			return ChunkTask{}, false
		case <-q.notify:
		}
	}
}

func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.Lock()
	if q.closed || (q.capacity > 0 && len(q.tasks) >= q.capacity) {
		q.mu.Unlock()
		return false
	}
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
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

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.mu.Unlock()
	close(q.closedChan)
}

