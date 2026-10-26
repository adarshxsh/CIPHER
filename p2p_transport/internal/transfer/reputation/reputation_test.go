package reputation_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/transfer/reputation"
)

func generateTestPeer(idStr string) peer.ID {
	return peer.ID(idStr)
}

func TestReputationManager_IntegrityFailureAndQuarantine(t *testing.T) {
	cfg := reputation.ReputationConfig{
		BaseBackoff:         50 * time.Millisecond,
		MaxBackoff:          200 * time.Millisecond,
		BackoffMultiplier:   2.0,
		QuarantineThreshold: 3,
		PenaltyIntegrity:    10,
		PenaltyTransient:    1,
	}

	rm := reputation.NewReputationManager(cfg)
	p := generateTestPeer("peer1")

	// Initial state
	if rm.ShouldSkip(p) {
		t.Fatal("New peer should not be skipped")
	}

	// First integrity failure
	rm.RecordIntegrityFailure(p)
	if rm.GetScore(p) != 10 {
		t.Fatalf("Expected penalty score 10, got %d", rm.GetScore(p))
	}
	if !rm.IsBackedOff(p) {
		t.Fatal("Peer should be backed off after failure 1")
	}
	if rm.IsQuarantined(p) {
		t.Fatal("Peer should not be quarantined after failure 1")
	}

	// Second integrity failure
	rm.RecordIntegrityFailure(p)
	if rm.GetScore(p) != 20 {
		t.Fatalf("Expected penalty score 20, got %d", rm.GetScore(p))
	}

	// Third integrity failure -> Should trigger quarantine
	rm.RecordIntegrityFailure(p)
	if rm.GetScore(p) != 30 {
		t.Fatalf("Expected penalty score 30, got %d", rm.GetScore(p))
	}
	if !rm.IsQuarantined(p) {
		t.Fatal("Peer should be quarantined after 3 integrity failures")
	}
	if !rm.ShouldSkip(p) {
		t.Fatal("Quarantined peer should be skipped")
	}
}

func TestReputationManager_ExponentialBackoffAndRecovery(t *testing.T) {
	cfg := reputation.ReputationConfig{
		BaseBackoff:         20 * time.Millisecond,
		MaxBackoff:          100 * time.Millisecond,
		BackoffMultiplier:   3.0,
		QuarantineThreshold: 5,
		PenaltyIntegrity:    5,
		PenaltyTransient:    1,
	}

	rm := reputation.NewReputationManager(cfg)
	p := generateTestPeer("peer2")

	rm.RecordTransientFailure(p)
	if !rm.IsBackedOff(p) {
		t.Fatal("Peer should be backed off")
	}

	// Wait for backoff to expire
	time.Sleep(30 * time.Millisecond)
	if rm.IsBackedOff(p) {
		t.Fatal("Peer backoff should have expired")
	}

	// Record success should reset consecutive failures
	rm.RecordSuccess(p)
	state, ok := rm.GetPeerState(p)
	if !ok {
		t.Fatal("Peer state not found")
	}
	if state.ConsecutiveFailures != 0 {
		t.Fatalf("Expected consecutive failures 0, got %d", state.ConsecutiveFailures)
	}
	if state.Successes != 1 {
		t.Fatalf("Expected 1 success, got %d", state.Successes)
	}
}

func TestReputationManager_ConcurrentAccess(t *testing.T) {
	rm := reputation.NewReputationManager()
	var wg sync.WaitGroup

	numGoroutines := 20
	opsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		p := generateTestPeer(fmt.Sprintf("peer_%d", i%5))
		go func(peerID peer.ID) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if j%2 == 0 {
					rm.RecordIntegrityFailure(peerID)
				} else if j%3 == 0 {
					rm.RecordTransientFailure(peerID)
				} else {
					rm.RecordSuccess(peerID)
				}
				_ = rm.ShouldSkip(peerID)
				_ = rm.GetScore(peerID)
			}
		}(p)
	}

	wg.Wait()
}
