package reputation_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/reputation"
)

func generateTestPeerID(t *testing.T, idStr string) peer.ID {
	t.Helper()
	pID, err := peer.Decode("12D3KooW" + fmt.Sprintf("%-44s", idStr)[:44])
	if err != nil {
		// Fallback to plain string cast for mock testing
		return peer.ID(idStr)
	}
	return pID
}

func TestPeerTracker_RecordHashFailureAndBlacklist(t *testing.T) {
	cfg := reputation.Config{
		InitialScore:    100,
		PenaltyPerError: 35,
		MaxErrors:       3,
		MinScore:        0,
		MaxTrackedPeers: 100,
	}
	tracker := reputation.NewPeerTracker(cfg)
	pID := peer.ID("test-peer-1")

	if tracker.IsBlacklisted(pID) {
		t.Fatal("New peer should not be blacklisted")
	}

	if score := tracker.GetScore(pID); score != 100 {
		t.Fatalf("Expected initial score 100, got %d", score)
	}

	// First failure
	score, blacklisted := tracker.RecordHashFailure(pID)
	if score != 65 || blacklisted {
		t.Fatalf("Expected score 65, blacklisted false; got score %d, blacklisted %v", score, blacklisted)
	}
	if tracker.GetHashFailures(pID) != 1 {
		t.Fatalf("Expected hash failures 1, got %d", tracker.GetHashFailures(pID))
	}

	// Second failure
	score, blacklisted = tracker.RecordHashFailure(pID)
	if score != 30 || blacklisted {
		t.Fatalf("Expected score 30, blacklisted false; got score %d, blacklisted %v", score, blacklisted)
	}

	// Third failure -> exceeds MaxErrors threshold
	score, blacklisted = tracker.RecordHashFailure(pID)
	if !blacklisted {
		t.Fatalf("Expected peer to be blacklisted after 3 failures")
	}
	if !tracker.IsBlacklisted(pID) {
		t.Fatalf("IsBlacklisted returned false for blacklisted peer")
	}

	stats, ok := tracker.GetPeerStats(pID)
	if !ok {
		t.Fatalf("GetPeerStats returned false")
	}
	if stats.HashFailures != 3 || stats.ErrorCount != 3 || !stats.Blacklisted {
		t.Fatalf("Unexpected stats: %+v", stats)
	}
}

func TestPeerTracker_ResetAndExplicitBlacklist(t *testing.T) {
	tracker := reputation.NewPeerTracker()
	pID := peer.ID("test-peer-2")

	tracker.Blacklist(pID)
	if !tracker.IsBlacklisted(pID) {
		t.Fatalf("Expected peer to be blacklisted")
	}

	tracker.Reset(pID)
	if tracker.IsBlacklisted(pID) {
		t.Fatalf("Expected peer to not be blacklisted after Reset")
	}
	if score := tracker.GetScore(pID); score != 100 {
		t.Fatalf("Expected score reset to 100, got %d", score)
	}
}

func TestPeerTracker_BoundedMemoryFootprint(t *testing.T) {
	cfg := reputation.Config{
		InitialScore:    100,
		PenaltyPerError: 35,
		MaxErrors:       3,
		MinScore:        0,
		MaxTrackedPeers: 10,
	}
	tracker := reputation.NewPeerTracker(cfg)

	// Add 10 peers
	for i := 0; i < 10; i++ {
		pID := peer.ID(fmt.Sprintf("peer-%d", i))
		tracker.RecordError(pID)
	}

	// Blacklist peer-0 so it shouldn't be evicted
	blacklistedPeer := peer.ID("peer-0")
	tracker.Blacklist(blacklistedPeer)

	// Add 10 more peers causing evictions
	for i := 10; i < 20; i++ {
		pID := peer.ID(fmt.Sprintf("peer-%d", i))
		tracker.RecordError(pID)
	}

	// Blacklisted peer MUST still be present
	if !tracker.IsBlacklisted(blacklistedPeer) {
		t.Fatalf("Blacklisted peer was unexpectedly evicted")
	}
}

func TestPeerTracker_ConcurrentAccess(t *testing.T) {
	tracker := reputation.NewPeerTracker()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pID := peer.ID(fmt.Sprintf("peer-%d", id%5))
			for j := 0; j < 100; j++ {
				tracker.RecordHashFailure(pID)
				_ = tracker.IsBlacklisted(pID)
				_ = tracker.GetScore(pID)
				_, _ = tracker.GetPeerStats(pID)
			}
		}(i)
	}

	wg.Wait()
}
