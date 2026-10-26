package scheduler_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/scheduler"
)

func TestReputationTracker_InitialState(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	peerID := "peer1"

	if tracker.IsQuarantined(peerID) {
		t.Fatalf("expected peer %s not to be quarantined initially", peerID)
	}

	if score := tracker.GetFaultScore(peerID); score != 0.0 {
		t.Fatalf("expected fault score 0.0, got %f", score)
	}
}

func TestReputationTracker_CorruptionQuarantine(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	peerID := "bad-peer"

	quarantined := tracker.RecordFailure(peerID, chunk.ErrChunkCorrupted)
	if !quarantined {
		t.Fatalf("expected peer %s to be quarantined immediately on corruption", peerID)
	}

	if !tracker.IsQuarantined(peerID) {
		t.Fatalf("expected IsQuarantined to return true for %s", peerID)
	}

	if score := tracker.GetFaultScore(peerID); score < 50.0 {
		t.Fatalf("expected fault score >= 50.0, got %f", score)
	}
}

func TestReputationTracker_GeneralErrorsAndSuccess(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	peerID := "flaky-peer"

	// General network error: +25.0 penalty (not quarantined yet)
	generalErr := errors.New("network timeout")
	quarantined := tracker.RecordFailure(peerID, generalErr)
	if quarantined {
		t.Fatalf("expected peer %s not to be quarantined after single general error", peerID)
	}
	if score := tracker.GetFaultScore(peerID); score != 25.0 {
		t.Fatalf("expected fault score 25.0, got %f", score)
	}

	// Success rewards: -5.0 reward
	tracker.RecordSuccess(peerID)
	if score := tracker.GetFaultScore(peerID); score != 20.0 {
		t.Fatalf("expected fault score 20.0 after success, got %f", score)
	}

	// Another general error: +25.0 penalty (total 45.0)
	tracker.RecordFailure(peerID, generalErr)
	if tracker.IsQuarantined(peerID) {
		t.Fatalf("expected peer %s not to be quarantined at score 45.0", peerID)
	}

	// Third general error: +25.0 penalty (total 70.0 >= 50.0) -> quarantined!
	quarantined = tracker.RecordFailure(peerID, generalErr)
	if !quarantined {
		t.Fatalf("expected peer %s to be quarantined after reaching score >= 50.0", peerID)
	}
}

func TestReputationTracker_Concurrent(t *testing.T) {
	tracker := scheduler.NewReputationTracker()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			peerID := fmt.Sprintf("peer-%d", id%5)
			if id%2 == 0 {
				tracker.RecordSuccess(peerID)
			} else {
				tracker.RecordFailure(peerID, errors.New("temp error"))
			}
			_ = tracker.IsQuarantined(peerID)
			_ = tracker.GetFaultScore(peerID)
		}(i)
	}

	wg.Wait()
}
