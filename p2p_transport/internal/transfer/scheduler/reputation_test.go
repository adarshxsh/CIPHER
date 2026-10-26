package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cipher/internal/protocol/chunk"
)

func TestReputationManager_BasicTransitions(t *testing.T) {
	cfg := ReputationConfig{
		InitialScore:           100.0,
		MaxScore:               100.0,
		SuccessReward:          10.0,
		ChecksumPenalty:        35.0,
		TimeoutPenalty:         15.0,
		CooldownThreshold:      50.0,
		BlacklistThreshold:     0.0,
		MaxConsecutiveFailures: 3,
		BaseBackoff:            10 * time.Millisecond,
		MaxBackoff:             100 * time.Millisecond,
	}

	rm := NewReputationManager(cfg)
	peerID := "peer1"

	// Initially active with score 100
	if status := rm.GetStatus(peerID); status != StatusActive {
		t.Fatalf("expected StatusActive, got %s", status)
	}
	if score := rm.GetScore(peerID); score != 100.0 {
		t.Fatalf("expected score 100.0, got %f", score)
	}

	// First checksum failure
	rm.RecordChecksumFailure(peerID)
	if score := rm.GetScore(peerID); score != 65.0 {
		t.Fatalf("expected score 65.0, got %f", score)
	}
	if status := rm.GetStatus(peerID); status != StatusCooldown {
		t.Fatalf("expected StatusCooldown after 1 failure, got %s", status)
	}

	// Second checksum failure
	rm.RecordChecksumFailure(peerID)
	if score := rm.GetScore(peerID); score != 30.0 {
		t.Fatalf("expected score 30.0, got %f", score)
	}

	// Third checksum failure - should trigger blacklist
	rm.RecordChecksumFailure(peerID)
	if score := rm.GetScore(peerID); score != -5.0 {
		t.Fatalf("expected score -5.0, got %f", score)
	}
	if status := rm.GetStatus(peerID); status != StatusBlacklisted {
		t.Fatalf("expected StatusBlacklisted after 3 consecutive failures, got %s", status)
	}
	if !rm.IsBlacklisted(peerID) {
		t.Fatalf("expected IsBlacklisted to be true")
	}
	if !rm.IsExcluded(peerID) {
		t.Fatalf("expected IsExcluded to be true for blacklisted peer")
	}
}

func TestReputationManager_3ConsecutiveChecksumFailures(t *testing.T) {
	rm := NewReputationManager()
	peerID := "bad-peer-1"

	for i := 1; i <= 3; i++ {
		rm.RecordFailure(peerID, fmt.Errorf("corrupted chunk x: %w", chunk.ErrChunkIntegrityMismatch))
		if i < 3 {
			if rm.IsBlacklisted(peerID) {
				t.Fatalf("peer should not be blacklisted on attempt %d", i)
			}
		}
	}

	if !rm.IsBlacklisted(peerID) {
		t.Fatalf("peer should be blacklisted within 3 consecutive checksum failures")
	}
}

func TestReputationManager_RecoveryOnSuccess(t *testing.T) {
	cfg := ReputationConfig{
		InitialScore:           100.0,
		MaxScore:               100.0,
		SuccessReward:          15.0,
		ChecksumPenalty:        30.0,
		TimeoutPenalty:         30.0,
		CooldownThreshold:      80.0,
		BlacklistThreshold:     0.0,
		MaxConsecutiveFailures: 3,
		BaseBackoff:            5 * time.Millisecond,
		MaxBackoff:             50 * time.Millisecond,
	}

	rm := NewReputationManager(cfg)
	peerID := "peer2"

	// 1 failure -> score 70 (in cooldown because 70 < 80)
	rm.RecordTimeout(peerID)
	if score := rm.GetScore(peerID); score != 70.0 {
		t.Fatalf("expected score 70.0, got %f", score)
	}
	if status := rm.GetStatus(peerID); status != StatusCooldown {
		t.Fatalf("expected StatusCooldown, got %s", status)
	}

	// Wait for backoff to expire
	time.Sleep(10 * time.Millisecond)

	// Record success -> score 85 (above threshold 80) => recovers to Active
	rm.RecordSuccess(peerID)
	if score := rm.GetScore(peerID); score != 85.0 {
		t.Fatalf("expected score 85.0, got %f", score)
	}
	if status := rm.GetStatus(peerID); status != StatusActive {
		t.Fatalf("expected StatusActive after recovery, got %s", status)
	}
}

func TestReputationManager_ConcurrentAccess(t *testing.T) {
	rm := NewReputationManager()
	var wg sync.WaitGroup

	numGoroutines := 20
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			peerID := fmt.Sprintf("peer-%d", id%5)
			for j := 0; j < opsPerGoroutine; j++ {
				if j%3 == 0 {
					rm.RecordSuccess(peerID)
				} else if j%3 == 1 {
					rm.RecordChecksumFailure(peerID)
				} else {
					rm.RecordTimeout(peerID)
				}
				_ = rm.GetStatus(peerID)
				_ = rm.GetScore(peerID)
				_ = rm.IsExcluded(peerID)
			}
		}(i)
	}

	wg.Wait()
}

func TestReputationManager_RecordFailureClassification(t *testing.T) {
	rm := NewReputationManager()
	pIntegrity := "peer-integrity"
	pTimeout := "peer-timeout"

	rm.RecordFailure(pIntegrity, chunk.ErrChunkIntegrityMismatch)
	stats, ok := rm.GetStats(pIntegrity)
	if !ok || stats.ChecksumFailures != 1 {
		t.Fatalf("expected 1 ChecksumFailure for pIntegrity, got %+v", stats)
	}

	rm.RecordFailure(pTimeout, errors.New("connection reset by peer"))
	statsTimeout, ok := rm.GetStats(pTimeout)
	if !ok || statsTimeout.NetworkTimeouts != 1 {
		t.Fatalf("expected 1 NetworkTimeout for pTimeout, got %+v", statsTimeout)
	}
}
