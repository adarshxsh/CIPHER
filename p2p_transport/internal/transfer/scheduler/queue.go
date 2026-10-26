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
	tasks []ChunkTask
	mu    sync.Mutex
}

func NewChunkQueue(tasks []ChunkTask) *ChunkQueue {
	return &ChunkQueue{
		tasks: tasks,
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

func (q *ChunkQueue) Push(task ChunkTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
}

func (q *ChunkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *ChunkQueue) HasOnlyMissedFor(peerID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return false
	}
	for _, task := range q.tasks {
		if task.MissedPeers == nil || !task.MissedPeers[peerID] {
			return false
		}
	}
	return true
}

type SourceQueue struct {
	sources []Source
	mu      sync.Mutex
}

func NewSourceQueue(sources []Source) *SourceQueue {
	srcs := make([]Source, len(sources))
	copy(srcs, sources)
	return &SourceQueue{
		sources: srcs,
	}
}

func (sq *SourceQueue) Pop() (Source, bool) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if len(sq.sources) == 0 {
		return Source{}, false
	}
	src := sq.sources[0]
	sq.sources = sq.sources[1:]
	return src, true
}

func (sq *SourceQueue) Push(src Source) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.sources = append(sq.sources, src)
}

func (sq *SourceQueue) Len() int {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return len(sq.sources)
}
