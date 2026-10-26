package reputation

import (
	"math"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTrackerBasicOperations(t *testing.T) {
	p1 := peer.ID("peer-1")
	p2 := peer.ID("peer-2")

	tracker := NewPeerReputationTracker()

	if tracker.IsBanned(p1) {
		t.Fatalf("expected p1 not to be banned initially")
	}

	if score := tracker.GetScore(p1); score != 0.0 {
		t.Fatalf("expected initial score 0.0, got %f", score)
	}

	// Success rewards
	tracker.RecordSuccess(p1)
	if score := tracker.GetScore(p1); score != 1.0 {
		t.Fatalf("expected score 1.0 after success, got %f", score)
	}

	// Connection failure penalty
	tracker.RecordConnectionFailure(p1)
	if score := tracker.GetScore(p1); score != 0.0 {
		t.Fatalf("expected score 0.0 after connection failure, got %f", score)
	}

	// Integrity fault penalty (-5.0)
	tracker.RecordIntegrityFault(p1)
	if score := tracker.GetScore(p1); score != -5.0 {
		t.Fatalf("expected score -5.0 after integrity fault, got %f", score)
	}

	if !tracker.IsBanned(p1) {
		t.Fatalf("expected p1 to be banned after negative score")
	}

	// P2 unaffected
	if tracker.IsBanned(p2) {
		t.Fatalf("expected p2 not to be banned")
	}

	tracker.ResetPeer(p1)
	if tracker.IsBanned(p1) {
		t.Fatalf("expected p1 not to be banned after reset")
	}
}

func TestTrackerMaxScoreCap(t *testing.T) {
	p1 := peer.ID("peer-1")
	cfg := DefaultTrackerConfig()
	cfg.MaxScore = 3.0
	tracker := NewPeerReputationTracker(cfg)

	for i := 0; i < 10; i++ {
		tracker.RecordSuccess(p1)
	}

	if score := tracker.GetScore(p1); score != 3.0 {
		t.Fatalf("expected score capped at 3.0, got %f", score)
	}
}

func TestTrackerConcurrency(t *testing.T) {
	tracker := NewPeerReputationTracker()
	p := peer.ID("concurrency-peer")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			tracker.RecordSuccess(p)
		}()
		go func() {
			defer wg.Done()
			tracker.RecordConnectionFailure(p)
		}()
		go func() {
			defer wg.Done()
			_ = tracker.IsBanned(p)
		}()
	}
	wg.Wait()

	_ = tracker.GetScore(p)
}

func TestFloatingPointTolerance(t *testing.T) {
	tracker := NewPeerReputationTracker()
	p := peer.ID("peer-float")

	tracker.RecordSuccess(p)
	tracker.RecordConnectionFailure(p)

	score := tracker.GetScore(p)
	if math.Abs(score-0.0) > 1e-6 {
		t.Fatalf("expected score near 0.0, got %f", score)
	}
}
