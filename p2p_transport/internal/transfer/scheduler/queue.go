package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"cipher/internal/content/core"
)

var ErrQueueFull = errors.New("queue full: maximum capacity reached")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
	AvailableAt time.Time
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	closed   bool
	mu       sync.Mutex
	notify   chan struct{}
	closeCh  chan struct{}
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	cap := len(tasks) * 3
	if cap < 10 {
		cap = 10
	}
	return NewChunkQueueWithCapacity(tasks, cap)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	qTasks := make([]ChunkTask, len(tasks))
	copy(qTasks, tasks)
	return &ChunkQueue{
		tasks:    qTasks,
		capacity: capacity,
		notify:   make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
	}
}

func (q *ChunkQueue) Push(task ChunkTask) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.New("queue closed")
	}
	if q.capacity > 0 && len(q.tasks) >= q.capacity {
		q.mu.Unlock()
		return ErrQueueFull
	}
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

func (q *ChunkQueue) Next(ctx context.Context, peerID string) (ChunkTask, bool) {
	for {
		if ctx.Err() != nil {
			return ChunkTask{}, false
		}

		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}

		var minWait time.Duration
		foundCandidate := false
		candidateIdx := -1

		now := time.Now()
		for i, task := range q.tasks {
			if peerID != "" && task.MissedPeers != nil && task.MissedPeers[peerID] {
				continue
			}
			foundCandidate = true

			if task.AvailableAt.IsZero() || !now.Before(task.AvailableAt) {
				candidateIdx = i
				break
			}

			wait := task.AvailableAt.Sub(now)
			if minWait == 0 || wait < minWait {
				minWait = wait
			}
		}

		if candidateIdx >= 0 {
			task := q.tasks[candidateIdx]
			q.tasks = append(q.tasks[:candidateIdx], q.tasks[candidateIdx+1:]...)
			q.mu.Unlock()
			return task, true
		}

		q.mu.Unlock()

		if !foundCandidate {
			// No tasks in queue eligible for this peerID
			return ChunkTask{}, false
		}

		// Found candidate task(s), but all are in exponential backoff delay.
		timer := time.NewTimer(minWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ChunkTask{}, false
		case <-q.closeCh:
			timer.Stop()
			return ChunkTask{}, false
		case <-q.notify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (q *ChunkQueue) NextTask() (ChunkTask, bool) {
	return q.Next(context.Background(), "")
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.closeCh)
	}
	q.mu.Unlock()
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) Capacity() int {
	return q.capacity
}

