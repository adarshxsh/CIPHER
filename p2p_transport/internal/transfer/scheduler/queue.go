package scheduler

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
	FailedPeers map[peer.ID]struct{}
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
	return q.NextForPeer("", nil)
}

func (q *ChunkQueue) NextForPeer(p peer.ID, tracker *PeerTracker) (ChunkTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return ChunkTask{}, false
	}

	if p != "" {
		pStr := p.String()
		for i, task := range q.tasks {
			if task.FailedPeers != nil {
				if _, failed := task.FailedPeers[p]; failed {
					continue
				}
			}
			if task.MissedPeers != nil {
				if task.MissedPeers[pStr] {
					continue
				}
			}
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return task, true
		}
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
