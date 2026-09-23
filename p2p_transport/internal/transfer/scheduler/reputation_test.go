package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestInitialScore(t *testing.T) {
	rep := NewReputationTracker()
	score := rep.GetScore("peer1")
	if score != 100.0 {
		t.Fatalf("Expected initial score 100.0, got %f", score)
	}
	if rep.IsExcluded("peer1") {
		t.Fatalf("Expected peer1 not to be excluded initially")
	}
}

func TestRecordEventsAndScoring(t *testing.T) {
	rep := NewReputationTracker()
	peerID := "peer1"

	// Record successes
	for i := 0; i < 5; i++ {
		rep.RecordEvent(peerID, EventSuccess)
	}
	score := rep.GetScore(peerID)
	if score != 100.0 {
		t.Fatalf("Expected score 100.0 after successes, got %f", score)
	}

	// Record 1 Hash Mismatch
	rep.RecordEvent(peerID, EventHashMismatch)
	score1 := rep.GetScore(peerID)
	if score1 >= 100.0 {
		t.Fatalf("Expected score to drop after hash mismatch, got %f", score1)
	}

	// Record 2nd error
	rep.RecordEvent(peerID, EventTimeout)
	score2 := rep.GetScore(peerID)
	if score2 >= score1 {
		t.Fatalf("Expected score to drop further after 2nd error, got %f (prev %f)", score2, score1)
	}
}

func TestAutomaticIsolationWithin3ConsecutiveErrors(t *testing.T) {
	t.Run("3 errors with no prior history", func(t *testing.T) {
		rep := NewReputationTracker()
		peerID := "bad-actor-1"

		rep.RecordEvent(peerID, EventHashMismatch)
		if rep.GetScore(peerID) >= 30.0 {
			t.Fatalf("Expected score < 30.0 after 1 error with 0 successes, got %f", rep.GetScore(peerID))
		}
		if !rep.IsExcluded(peerID) {
			t.Fatalf("Expected bad-actor-1 to be excluded")
		}
	})

	t.Run("3 errors with prior successes", func(t *testing.T) {
		rep := NewReputationTracker()
		peerID := "peer-with-history"

		// Record 20 successes
		for i := 0; i < 20; i++ {
			rep.RecordEvent(peerID, EventSuccess)
		}
		if rep.GetScore(peerID) != 100.0 {
			t.Fatalf("Expected score 100.0 before errors, got %f", rep.GetScore(peerID))
		}

		// 3 consecutive errors
		rep.RecordEvent(peerID, EventHashMismatch)
		rep.RecordEvent(peerID, EventTimeout)
		rep.RecordEvent(peerID, EventHashMismatch)

		score := rep.GetScore(peerID)
		if score >= 30.0 {
			t.Fatalf("Expected score < 30.0 after 3 consecutive errors, got %f", score)
		}
		if !rep.IsExcluded(peerID) {
			t.Fatalf("Expected peer to be excluded after 3 consecutive errors")
		}
	})
}

func TestSlidingWindowExpiration(t *testing.T) {
	window := 5 * time.Minute
	rep := NewReputationTracker(WithWindow(window))
	peerID := "peer1"

	now := time.Now()
	sixMinutesAgo := now.Add(-6 * time.Minute)

	// Record 3 errors 6 minutes ago
	rep.RecordEventAt(peerID, EventHashMismatch, sixMinutesAgo)
	rep.RecordEventAt(peerID, EventTimeout, sixMinutesAgo)
	rep.RecordEventAt(peerID, EventHashMismatch, sixMinutesAgo)

	// Old events should be outside 5-minute sliding window at `now`
	score := rep.GetScoreAt(peerID, now)
	if score != 100.0 {
		t.Fatalf("Expected score 100.0 after window expiration, got %f", score)
	}
	if rep.IsExcluded(peerID) {
		t.Fatalf("Expected peer not to be excluded after window expiration")
	}
}

func TestMemoryCapAndEviction(t *testing.T) {
	maxPeers := 100
	rep := NewReputationTracker(WithMaxPeers(maxPeers))

	for i := 0; i < 150; i++ {
		peerID := fmt.Sprintf("peer-%d", i)
		rep.RecordEvent(peerID, EventSuccess)
	}

	count := rep.CountPeers()
	if count > maxPeers {
		t.Fatalf("Expected active peers <= %d, got %d", maxPeers, count)
	}
}

func TestConcurrentAccess(t *testing.T) {
	rep := NewReputationTracker()
	var wg sync.WaitGroup

	numGoroutines := 10
	numOps := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			peerID := fmt.Sprintf("peer-%d", gid%3)
			for i := 0; i < numOps; i++ {
				if i%3 == 0 {
					rep.RecordEvent(peerID, EventSuccess)
				} else if i%3 == 1 {
					rep.RecordEvent(peerID, EventHashMismatch)
				} else {
					rep.RecordEvent(peerID, EventTimeout)
				}
				_ = rep.GetScore(peerID)
				_ = rep.IsExcluded(peerID)
			}
		}(g)
	}

	wg.Wait()
}
