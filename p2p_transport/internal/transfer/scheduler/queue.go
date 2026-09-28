package scheduler

import (
	"errors"
	"sync"

	"cipher/internal/content/core"
)

const DefaultMaxQueueCapacity = 10000

var ErrQueueFull = errors.New("chunk queue capacity limit reached")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	mu       sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	cap := len(tasks) * 3
	if cap == 0 {
		cap = DefaultMaxQueueCapacity
	}
	return NewChunkQueueWithCapacity(tasks, cap)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity <= 0 {
		capacity = DefaultMaxQueueCapacity
	}
	buf := make([]ChunkTask, 0, capacity)
	buf = append(buf, tasks...)
	return &ChunkQueue{
		tasks:    buf,
		capacity: capacity,
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

func (q *ChunkQueue) NextForPeer(peerID string) (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}
	for i, task := range q.tasks {
		if task.MissedPeers == nil || !task.MissedPeers[peerID] {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task, true
		}
	}
	return ChunkTask{}, false
}

func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.capacity > 0 && len(q.tasks) >= q.capacity {
		return false
	}
	q.tasks = append(q.tasks, task)
	return true
}

func (q *ChunkQueue) Cap() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) Depth() int {
	return q.Len()
}
