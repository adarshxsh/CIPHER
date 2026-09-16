package scheduler

import (
	"errors"
	"sync"

	"cipher/internal/content/core"
)

const DefaultMaxQueueCapacity = 10000

var ErrQueueFull = errors.New("task queue is at maximum capacity")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks       []ChunkTask
	maxCapacity int
	mu          sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return NewChunkQueueWithCapacity(tasks, DefaultMaxQueueCapacity)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, maxCapacity int) *ChunkQueue {
	if maxCapacity <= 0 {
		maxCapacity = DefaultMaxQueueCapacity
	}
	n := len(tasks)
	if n > maxCapacity {
		n = maxCapacity
	}
	t := make([]ChunkTask, n)
	if n > 0 {
		copy(t, tasks[:n])
	}
	return &ChunkQueue{
		tasks:       t,
		maxCapacity: maxCapacity,
	}
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

func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.maxCapacity > 0 && len(q.tasks) >= q.maxCapacity {
		return false
	}
	q.tasks = append(q.tasks, task)
	return true
}

func (q *ChunkQueue) PushWithError(task ChunkTask) error {
	if !q.Push(task) {
		return ErrQueueFull
	}
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
	return q.maxCapacity
}
