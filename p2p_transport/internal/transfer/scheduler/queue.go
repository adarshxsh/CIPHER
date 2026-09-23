package scheduler

import (
	"context"
	"errors"
	"sync"

	"cipher/internal/content/core"
)

var ErrQueueFull = errors.New("chunk queue capacity exceeded")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	closed   bool
	notify   chan struct{}
	mu       sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	cap := len(tasks) * 2
	if cap < 1024 {
		cap = 1024
	}
	return NewChunkQueueWithCapacity(tasks, cap)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity < len(tasks) {
		capacity = len(tasks)
	}
	t := make([]ChunkTask, len(tasks), capacity)
	copy(t, tasks)
	return &ChunkQueue{
		tasks:    t,
		capacity: capacity,
		notify:   make(chan struct{}, 1),
	}
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	return q.NextForPeer(context.Background(), "")
}

func (q *ChunkQueue) NextForPeer(ctx context.Context, peerID string) (ChunkTask, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}

		idx := -1
		for i, t := range q.tasks {
			if peerID == "" || t.MissedPeers == nil || !t.MissedPeers[peerID] {
				idx = i
				break
			}
		}

		if idx != -1 {
			task := q.tasks[idx]
			q.tasks = append(q.tasks[:idx], q.tasks[idx+1:]...)
			if len(q.tasks) > 0 {
				select {
				case q.notify <- struct{}{}:
				default:
				}
			}
			q.mu.Unlock()
			return task, true
		}

		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ChunkTask{}, false
		case <-q.notify:
			// task pushed or queue closed; retry finding eligible task
		}
	}
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return errors.New("queue closed")
	}

	if len(q.tasks) >= q.capacity {
		return ErrQueueFull
	}

	q.tasks = append(q.tasks, task)

	select {
	case q.notify <- struct{}{}:
	default:
	}

	return nil
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

func (q *ChunkQueue) Capacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

