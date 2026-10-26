package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/engine"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

type Source struct {
	PeerID    peer.ID
	Available map[core.ChunkID]struct{}
}

type PeerScore struct {
	Successes           int
	Failures            int
	ConsecutiveFailures int
	Score               float64
	Blacklisted         bool
	LastFailure         time.Time
	LastSuccess         time.Time
	LastDecay           time.Time
}

type PeerReputationTracker struct {
	mu             sync.RWMutex
	scores         map[peer.ID]*PeerScore
	maxFailures    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	decayInterval  time.Duration
}

func NewPeerReputationTracker() *PeerReputationTracker {
	return &PeerReputationTracker{
		scores:         make(map[peer.ID]*PeerScore),
		maxFailures:    3,
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     5 * time.Second,
		decayInterval:  30 * time.Second,
	}
}

func (rt *PeerReputationTracker) SetMaxFailures(max int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.maxFailures = max
}

func (rt *PeerReputationTracker) SetBackoffParams(initial, max time.Duration) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.initialBackoff = initial
	rt.maxBackoff = max
}

func (rt *PeerReputationTracker) SetDecayInterval(d time.Duration) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.decayInterval = d
}

func (rt *PeerReputationTracker) RecordSuccess(p peer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	score, exists := rt.scores[p]
	if !exists {
		score = &PeerScore{Score: 100.0, LastDecay: time.Now()}
		rt.scores[p] = score
	}

	score.Successes++
	score.ConsecutiveFailures = 0
	score.Score += 10.0
	if score.Score > 100.0 {
		score.Score = 100.0
	}
	score.LastSuccess = time.Now()
}

func (rt *PeerReputationTracker) RecordFailure(p peer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	score, exists := rt.scores[p]
	if !exists {
		score = &PeerScore{Score: 100.0, LastDecay: time.Now()}
		rt.scores[p] = score
	}

	score.Failures++
	score.ConsecutiveFailures++
	score.Score -= 25.0
	if score.Score < 0 {
		score.Score = 0
	}
	score.LastFailure = time.Now()

	if score.ConsecutiveFailures >= rt.maxFailures {
		score.Blacklisted = true
	}
}

func (rt *PeerReputationTracker) IsBlacklisted(p peer.ID) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.decayIfNeededLocked(p)
	score, exists := rt.scores[p]
	if !exists {
		return false
	}
	return score.Blacklisted
}

func (rt *PeerReputationTracker) GetBackoff(p peer.ID) time.Duration {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.decayIfNeededLocked(p)
	score, exists := rt.scores[p]
	if !exists || score.ConsecutiveFailures == 0 {
		return 0
	}

	shift := score.ConsecutiveFailures - 1
	if shift > 30 {
		shift = 30
	}
	backoff := rt.initialBackoff << shift
	if backoff > rt.maxBackoff || backoff <= 0 {
		backoff = rt.maxBackoff
	}
	return backoff
}

func (rt *PeerReputationTracker) GetScore(p peer.ID) float64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.decayIfNeededLocked(p)
	score, exists := rt.scores[p]
	if !exists {
		return 100.0
	}
	return score.Score
}

func (rt *PeerReputationTracker) GetConsecutiveFailures(p peer.ID) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.decayIfNeededLocked(p)
	score, exists := rt.scores[p]
	if !exists {
		return 0
	}
	return score.ConsecutiveFailures
}

func (rt *PeerReputationTracker) DecayScores() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := time.Now()
	for _, score := range rt.scores {
		rt.applyDecayLocked(score, now)
	}
}

func (rt *PeerReputationTracker) decayIfNeededLocked(p peer.ID) {
	score, exists := rt.scores[p]
	if !exists {
		return
	}
	rt.applyDecayLocked(score, time.Now())
}

func (rt *PeerReputationTracker) applyDecayLocked(score *PeerScore, now time.Time) {
	if rt.decayInterval <= 0 {
		return
	}
	if score.LastDecay.IsZero() {
		score.LastDecay = now
		return
	}
	elapsed := now.Sub(score.LastDecay)
	if elapsed >= rt.decayInterval {
		intervals := int(elapsed / rt.decayInterval)
		score.LastDecay = score.LastDecay.Add(time.Duration(intervals) * rt.decayInterval)

		score.ConsecutiveFailures -= intervals
		if score.ConsecutiveFailures < 0 {
			score.ConsecutiveFailures = 0
		}
		if score.ConsecutiveFailures < rt.maxFailures {
			score.Blacklisted = false
		}
		score.Score += float64(intervals * 10)
		if score.Score > 100.0 {
			score.Score = 100.0
		}
	}
}

type Scheduler struct {
	Transport   *transport.Transport
	Engine      *engine.ContentEngine
	MaxAttempts int
	Tracker     *PeerReputationTracker
}

func NewScheduler(t *transport.Transport, eng *engine.ContentEngine, maxAttempts int) *Scheduler {
	return &Scheduler{
		Transport:   t,
		Engine:      eng,
		MaxAttempts: maxAttempts,
		Tracker:     NewPeerReputationTracker(),
	}
}

func (s *Scheduler) Run(ctx context.Context, tasks []ChunkTask, sources []Source, completions chan<- WorkerResult) error {
	if s.Tracker == nil {
		s.Tracker = NewPeerReputationTracker()
	}
	queue := NewChunkQueue(tasks)
	defer queue.Close()

	results := make(chan WorkerResult, len(sources)*2)

	// Start workers
	activeWorkers := 0
	for _, source := range sources {
		if s.Tracker.IsBlacklisted(source.PeerID) {
			log.Printf("[Scheduler] Skipping blacklisted peer: %s", source.PeerID)
			continue
		}
		client, err := chunk.NewClient(ctx, s.Transport, source.PeerID, s.Engine)
		if err != nil {
			log.Printf("[Scheduler] Warning: Failed to connect to source %s: %v", source.PeerID, err)
			continue
		}
		activeWorkers++
		go func(src Source, c *chunk.Client) {
			defer c.Close()
			runWorker(ctx, src, c, s.Engine, queue, results, s.Tracker)
			results <- WorkerResult{Error: fmt.Errorf("worker_done")} // Special signal
		}(source, client)
	}

	if activeWorkers == 0 {
		return fmt.Errorf("no active workers could be started")
	}

	pendingTasks := len(tasks)

	for pendingTasks > 0 && activeWorkers > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-results:
			if res.Error != nil {
				if res.Error.Error() == "worker_done" {
					activeWorkers--
					continue
				}
				// If provider returned ErrChunkNotFound, this is an expected candidate miss in a partial-replica CDN
				if errors.Is(res.Error, chunk.ErrRemoteChunkNotFound) {
					if res.Task.MissedPeers == nil {
						res.Task.MissedPeers = make(map[string]bool)
					}
					res.Task.MissedPeers[res.PeerID] = true

					if len(res.Task.MissedPeers) < len(sources) {
						queue.Push(res.Task)
						continue
					}
					return fmt.Errorf("chunk %x not found across any candidate providers (%d/%d checked)", res.Task.ChunkID, len(res.Task.MissedPeers), len(sources))
				}

				// Real network / integrity error: count attempts
				res.Task.Attempts++
				if res.Task.Attempts < s.MaxAttempts {
					queue.Push(res.Task)
				} else {
					return fmt.Errorf("chunk %x failed after %d attempts: %w", res.Task.ChunkID, s.MaxAttempts, res.Error)
				}
			} else {
				// Success
				completions <- res
				pendingTasks--
			}
		}
	}

	if pendingTasks > 0 {
		return fmt.Errorf("all workers died, %d chunks remaining", pendingTasks)
	}

	return nil
}
