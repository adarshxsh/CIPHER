package scheduler_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"cipher/internal/transfer/scheduler"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestPeerTracker_Scoring(t *testing.T) {
	tracker := scheduler.NewPeerTracker()
	peerID := peer.ID("test-peer-1")

	// Initial score should be 0
	if score := tracker.GetScore(peerID); score != 0 {
		t.Fatalf("Expected initial score 0, got %d", score)
	}

	// Record success (+5)
	tracker.RecordSuccess(peerID)
	if score := tracker.GetScore(peerID); score != 5 {
		t.Errorf("Expected score 5 after 1 success, got %d", score)
	}

	// Record timeout (-10)
	tracker.RecordTimeout(peerID)
	if score := tracker.GetScore(peerID); score != -5 {
		t.Errorf("Expected score -5 after timeout, got %d", score)
	}

	// Record integrity failure (-50)
	tracker.RecordIntegrityFailure(peerID)
	if score := tracker.GetScore(peerID); score != -55 {
		t.Errorf("Expected score -55 after integrity failure, got %d", score)
	}

	stats := tracker.GetStats(peerID)
	if stats.Successes != 1 || stats.Timeouts != 1 || stats.IntegrityFailures != 1 {
		t.Errorf("Unexpected stats breakdown: %+v", stats)
	}
}

func TestPeerTracker_Backoff(t *testing.T) {
	tracker := scheduler.NewPeerTracker()
	tracker.BaseBackoff = 10 * time.Millisecond
	peerID := peer.ID("test-peer-backoff")

	// Positive score -> no backoff
	tracker.RecordSuccess(peerID)
	if backoff := tracker.GetBackoff(peerID); backoff != 0 {
		t.Errorf("Expected 0 backoff for positive score, got %v", backoff)
	}

	// First timeout (-10 score, 1 consecutive failure)
	tracker.RecordTimeout(peerID)
	expectedBackoff1 := 10 * time.Millisecond // BaseBackoff * 2^0
	if backoff := tracker.GetBackoff(peerID); backoff != expectedBackoff1 {
		t.Errorf("Expected backoff %v, got %v", expectedBackoff1, backoff)
	}

	// Second timeout (-20 score, 2 consecutive failures)
	tracker.RecordTimeout(peerID)
	expectedBackoff2 := 20 * time.Millisecond // BaseBackoff * 2^1
	if backoff := tracker.GetBackoff(peerID); backoff != expectedBackoff2 {
		t.Errorf("Expected backoff %v, got %v", expectedBackoff2, backoff)
	}

	// Success resets consecutive failures and backoff
	tracker.RecordSuccess(peerID)
	if backoff := tracker.GetBackoff(peerID); backoff != 0 {
		t.Errorf("Expected 0 backoff after success, got %v", backoff)
	}
}

func TestPeerTracker_BanThreshold(t *testing.T) {
	tracker := scheduler.NewPeerTracker()
	tracker.MaxIntegrityFailures = 2
	tracker.BanThreshold = -100
	peerID := peer.ID("bad-peer")

	if tracker.IsBanned(peerID) {
		t.Fatal("Peer should not be banned initially")
	}

	// First integrity failure (-50 score)
	tracker.RecordIntegrityFailure(peerID)
	if tracker.IsBanned(peerID) {
		t.Fatal("Peer should not be banned after 1 integrity failure")
	}

	// Second integrity failure (-100 score, 2 failures)
	tracker.RecordIntegrityFailure(peerID)
	if !tracker.IsBanned(peerID) {
		t.Fatal("Peer should be banned after 2 integrity failures")
	}
}

func TestPeerTracker_ThreadSafety(t *testing.T) {
	tracker := scheduler.NewPeerTracker()
	peers := []peer.ID{"p1", "p2", "p3", "p4"}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := peers[rand.Intn(len(peers))]
			tracker.RecordSuccess(p)
			tracker.RecordTimeout(p)
			tracker.RecordIntegrityFailure(p)
			_ = tracker.GetScore(p)
			_ = tracker.IsBanned(p)
			_ = tracker.GetBackoff(p)
			_ = tracker.GetStats(p)
			_ = tracker.GetAllStats()
		}()
	}
	wg.Wait()
}
