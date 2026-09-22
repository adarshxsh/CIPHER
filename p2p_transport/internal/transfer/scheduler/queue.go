package scheduler

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/reputation"
)

type ChunkTask struct {
	Index       int
	ChunkID     core.ChunkID
	Attempts    int
	MissedPeers map[string]bool
	FailedPeers map[string]bool
}

type ChunkQueue struct {
	tasks    []ChunkTask
	inFlight int
	mu       sync.Mutex
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
	q.inFlight++
	return task, true
}

// PopForPeer retrieves the next task that peerID is eligible to execute.
// It skips tasks where peerID is listed in MissedPeers or FailedPeers.
// Returns (task, ok, shouldExit). If shouldExit is true, the caller worker loop should terminate.
func (q *ChunkQueue) PopForPeer(peerID peer.ID, tracker *reputation.Tracker) (ChunkTask, bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	peerStr := peerID.String()

	if tracker != nil && tracker.IsQuarantined(peerID) {
		return ChunkTask{}, false, true
	}

	eligibleIdx := -1
	for i, task := range q.tasks {
		if (task.MissedPeers != nil && task.MissedPeers[peerStr]) ||
			(task.FailedPeers != nil && task.FailedPeers[peerStr]) {
			continue
		}
		eligibleIdx = i
		break
	}

	if eligibleIdx >= 0 {
		task := q.tasks[eligibleIdx]
		q.tasks = append(q.tasks[:eligibleIdx], q.tasks[eligibleIdx+1:]...)
		q.inFlight++
		return task, true, false
	}

	if q.inFlight == 0 {
		return ChunkTask{}, false, true
	}

	return ChunkTask{}, false, false
}

func (q *ChunkQueue) FinishTask(task ChunkTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inFlight > 0 {
		q.inFlight--
	}
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

func (q *ChunkQueue) InFlight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inFlight
}
