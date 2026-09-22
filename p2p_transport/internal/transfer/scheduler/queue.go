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
	tasks    []ChunkTask
	mu       sync.Mutex
	notify   chan struct{}
	closedCh chan struct{}
	closed   bool
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return &ChunkQueue{
		tasks:    append([]ChunkTask(nil), tasks...),
		notify:   make(chan struct{}, 10000),
		closedCh: make(chan struct{}),
	}
}

func (q *ChunkQueue) Next(ctx context.Context) (ChunkTask, bool) {
	for {
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
		case <-q.closedCh:
			return ChunkTask{}, false
		case <-q.notify:
			// loop around
		}
	}
}

func (q *ChunkQueue) Push(task ChunkTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.tasks = append(q.tasks, task)
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.closedCh)
	}
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}
