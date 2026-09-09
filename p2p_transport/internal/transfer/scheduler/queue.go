package scheduler

import (
	"sync"

	"cipher/internal/content/core"
)

type ChunkTask struct {
	Index    int
	ChunkID  core.ChunkID
	Attempts int
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	closed   bool
	mu       sync.Mutex
	cond     *sync.Cond
}

func NewChunkQueue(tasks []ChunkTask, capacity ...int) *ChunkQueue {
	capVal := 0
	if len(capacity) > 0 {
		capVal = capacity[0]
		if capVal < 0 {
			capVal = 0
		}
	} else {
		capVal = len(tasks)
		if capVal < 100 {
			capVal = 100
		}
	}

	q := &ChunkQueue{
		tasks:    append([]ChunkTask(nil), tasks...),
		capacity: capVal,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	q.cond.Broadcast()
	return task, true
}

func (q *ChunkQueue) Push(task ChunkTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.capacity > 0 && len(q.tasks) >= q.capacity && !q.closed {
		q.cond.Wait()
	}

	if q.closed {
		return false
	}

	q.tasks = append(q.tasks, task)
	q.cond.Broadcast()
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

func (q *ChunkQueue) SetCapacity(capacity int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if capacity < 0 {
		capacity = 0
	}
	q.capacity = capacity
	q.cond.Broadcast()
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

