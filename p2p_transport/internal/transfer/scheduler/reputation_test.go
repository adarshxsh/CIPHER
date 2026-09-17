package scheduler_test

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/transfer/scheduler"
)

func TestReputationManager_FailureAndSuccessMetrics(t *testing.T) {
	peerID := peer.ID("test-peer-1")
	cfg := scheduler.ReputationConfig{
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  1 * time.Second,
	}
	rm := scheduler.NewReputationManager(cfg)

	// Initial state
	rep := rm.GetReputation(peerID)
	if rep.FailureCount != 0 || rep.ConsecutiveErrors != 0 || rep.Score != 1.0 {
		t.Errorf("Unexpected initial reputation: %+v", rep)
	}

	// Record 1st failure
	b1 := rm.RecordFailure(peerID)
	if b1 != 100*time.Millisecond {
		t.Errorf("Expected 100ms backoff, got %v", b1)
	}

	rep = rm.GetReputation(peerID)
	if rep.FailureCount != 1 || rep.ConsecutiveErrors != 1 {
		t.Errorf("Expected FailureCount=1, ConsecutiveErrors=1, got %+v", rep)
	}
	if rep.Score >= 1.0 {
		t.Errorf("Expected score to decrease on failure, got %f", rep.Score)
	}
	if !rm.IsInBackoff(peerID) {
		t.Error("Expected peer to be in backoff quarantine")
	}

	// Record 2nd failure (Exponential backoff 100ms * 2 = 200ms)
	b2 := rm.RecordFailure(peerID)
	if b2 != 200*time.Millisecond {
		t.Errorf("Expected 200ms backoff, got %v", b2)
	}

	rep = rm.GetReputation(peerID)
	if rep.FailureCount != 2 || rep.ConsecutiveErrors != 2 {
		t.Errorf("Expected FailureCount=2, ConsecutiveErrors=2, got %+v", rep)
	}

	// Record success -> Resets consecutive errors, restores score
	rm.RecordSuccess(peerID)
	rep = rm.GetReputation(peerID)
	if rep.ConsecutiveErrors != 0 {
		t.Errorf("Expected ConsecutiveErrors reset to 0, got %d", rep.ConsecutiveErrors)
	}
	if rep.SuccessCount != 1 {
		t.Errorf("Expected SuccessCount=1, got %d", rep.SuccessCount)
	}

	// Next failure after success should start back at BaseBackoff
	b3 := rm.RecordFailure(peerID)
	if b3 != 100*time.Millisecond {
		t.Errorf("Expected backoff to reset to 100ms after success, got %v", b3)
	}
}

func TestReputationManager_ExponentialBackoffCap(t *testing.T) {
	peerID := peer.ID("test-peer-cap")
	cfg := scheduler.ReputationConfig{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
	}
	rm := scheduler.NewReputationManager(cfg)

	b1 := rm.RecordFailure(peerID) // 10ms
	b2 := rm.RecordFailure(peerID) // 20ms
	b3 := rm.RecordFailure(peerID) // 40ms
	b4 := rm.RecordFailure(peerID) // capped at 50ms
	b5 := rm.RecordFailure(peerID) // capped at 50ms

	if b1 != 10*time.Millisecond || b2 != 20*time.Millisecond || b3 != 40*time.Millisecond {
		t.Errorf("Exponential sequence incorrect: b1=%v, b2=%v, b3=%v", b1, b2, b3)
	}
	if b4 != 50*time.Millisecond || b5 != 50*time.Millisecond {
		t.Errorf("Expected backoff capped at 50ms, got b4=%v, b5=%v", b4, b5)
	}
}

func TestReputationManager_ConcurrentAccess(t *testing.T) {
	rm := scheduler.NewReputationManager()
	peers := []peer.ID{"peer-a", "peer-b", "peer-c", "peer-d"}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := peers[idx%len(peers)]
			if idx%2 == 0 {
				rm.RecordFailure(p)
			} else {
				rm.RecordSuccess(p)
			}
			_ = rm.GetReputation(p)
			_ = rm.IsInBackoff(p)
			_ = rm.GetBackoffRemaining(p)
		}(i)
	}
	wg.Wait()
}
