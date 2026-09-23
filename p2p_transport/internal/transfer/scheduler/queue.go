package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"cipher/internal/content/core"
)

const DefaultMaxQueueCapacity = 10000

var ErrQueueFull = errors.New("queue capacity exceeded")

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
	ReadyAt     time.Time
}

type ChunkQueue struct {
	tasks    []ChunkTask
	capacity int
	closed   bool
	mu       sync.Mutex
	notifyCh chan struct{}
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return NewChunkQueueWithCapacity(tasks, DefaultMaxQueueCapacity)
}

func NewChunkQueueWithCapacity(tasks []ChunkTask, capacity int) *ChunkQueue {
	if capacity <= 0 {
		capacity = DefaultMaxQueueCapacity
	}
	q := &ChunkQueue{
		tasks:    make([]ChunkTask, 0, len(tasks)),
		capacity: capacity,
		notifyCh: make(chan struct{}),
	}
	for _, t := range tasks {
		_ = q.Push(t)
	}
	return q
}

func taskBefore(a, b ChunkTask) bool {
	if a.ReadyAt.IsZero() && !b.ReadyAt.IsZero() {
		return true
	}
	if !a.ReadyAt.IsZero() && !b.ReadyAt.IsZero() {
		return a.ReadyAt.Before(b.ReadyAt)
	}
	return false
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

	inserted := false
	for i, t := range q.tasks {
		if taskBefore(task, t) {
			q.tasks = append(q.tasks[:i], append([]ChunkTask{task}, q.tasks[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		q.tasks = append(q.tasks, task)
	}

	close(q.notifyCh)
	q.notifyCh = make(chan struct{})
	return nil
}

func (q *ChunkQueue) PopForPeer(ctx context.Context, peerID string) (ChunkTask, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ChunkTask{}, false
		}
		if ctx != nil && ctx.Err() != nil {
			q.mu.Unlock()
			return ChunkTask{}, false
		}

		eligibleIdx := -1
		for i, t := range q.tasks {
			if t.MissedPeers == nil || peerID == "" || !t.MissedPeers[peerID] {
				eligibleIdx = i
				break
			}
		}

		if eligibleIdx == -1 {
			// No task eligible for this peer right now
			notify := q.notifyCh
			q.mu.Unlock()

			if ctx == nil {
				return ChunkTask{}, false
			}

			select {
			case <-ctx.Done():
				return ChunkTask{}, false
			case <-notify:
				continue
			}
		}

		task := q.tasks[eligibleIdx]
		now := time.Now()

		if task.ReadyAt.IsZero() || !now.Before(task.ReadyAt) {
			// Task is ready
			q.tasks = append(q.tasks[:eligibleIdx], q.tasks[eligibleIdx+1:]...)
			q.mu.Unlock()
			return task, true
		}

		// Task is delayed
		wait := task.ReadyAt.Sub(now)
		notify := q.notifyCh
		q.mu.Unlock()

		if ctx == nil {
			return ChunkTask{}, false
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ChunkTask{}, false
		case <-notify:
			timer.Stop()
			continue
		case <-timer.C:
			continue
		}
	}
}

func (q *ChunkQueue) NextContext(ctx context.Context) (ChunkTask, bool) {
	return q.PopForPeer(ctx, "")
}

func (q *ChunkQueue) Next() (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || len(q.tasks) == 0 {
		return ChunkTask{}, false
	}

	now := time.Now()
	for i, t := range q.tasks {
		if t.ReadyAt.IsZero() || !now.Before(t.ReadyAt) {
			task := t
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task, true
		}
	}

	return ChunkTask{}, false
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
	return q.capacity
}

func (q *ChunkQueue) NotifyChan() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.notifyCh
}

func (q *ChunkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.notifyCh)
	}
}
