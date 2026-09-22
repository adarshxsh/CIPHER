package scheduler

import (
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
	tasks  []ChunkTask
	mu     sync.Mutex
	notify chan struct{}
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return &ChunkQueue{
		tasks:  tasks,
		notify: make(chan struct{}, 1),
	}
}

func (q *ChunkQueue) PopForPeer(peerID string) (ChunkTask, bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tasks) == 0 {
		return ChunkTask{}, false, false
	}

	for i, t := range q.tasks {
		if t.MissedPeers == nil || !t.MissedPeers[peerID] {
			task := t
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task, true, true
		}
	}

	return ChunkTask{}, false, true
}

func (q *ChunkQueue) HasTaskForPeer(peerID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.MissedPeers == nil || !t.MissedPeers[peerID] {
			return true
		}
	}
	return false
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

func (q *ChunkQueue) Push(task ChunkTask) {
	q.mu.Lock()
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) NotifyChan() <-chan struct{} {
	return q.notify
}
