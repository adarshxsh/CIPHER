package scheduler_test

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/transfer/scheduler"
)

func TestReputationManager_FaultAndBanning(t *testing.T) {
	cfg := scheduler.ReputationConfig{
		BanThreshold:  3.0,
		BanDuration:   100 * time.Millisecond,
		FaultWeight:   1.0,
		DecayHalfLife: 10 * time.Minute,
	}
	rm := scheduler.NewReputationManager(cfg)
	peerA := "peer-a"

	if rm.IsBanned(peerA) {
		t.Fatalf("Peer %s should not be banned initially", peerA)
	}

	rm.RecordFault(peerA)
	rm.RecordFault(peerA)

	if rm.IsBanned(peerA) {
		t.Fatalf("Peer %s should not be banned after 2 faults", peerA)
	}

	score := rm.GetScore(peerA)
	if math.Abs(score-2.0) >= 1e-6 {
		t.Fatalf("Expected score 2.0, got %f", score)
	}

	// 3rd fault triggers ban
	rm.RecordFault(peerA)
	if !rm.IsBanned(peerA) {
		t.Fatalf("Peer %s should be banned after 3 faults", peerA)
	}

	// Wait for ban duration to expire
	time.Sleep(150 * time.Millisecond)

	if rm.IsBanned(peerA) {
		t.Fatalf("Peer %s should no longer be banned after ban duration expired", peerA)
	}
}

func TestReputationManager_ScoreDecay(t *testing.T) {
	halfLife := 10 * time.Minute
	cfg := scheduler.ReputationConfig{
		BanThreshold:  10.0,
		BanDuration:   1 * time.Hour,
		FaultWeight:   4.0,
		DecayHalfLife: halfLife,
	}
	rm := scheduler.NewReputationManager(cfg)
	peerB := "peer-b"
	now := time.Now()

	rm.RecordFaultAt(peerB, now)

	score0 := rm.GetScoreAt(peerB, now)
	if math.Abs(score0-4.0) >= 1e-6 {
		t.Fatalf("Expected initial score 4.0, got %f", score0)
	}

	// After 1 half-life (10 minutes), score should be 2.0
	t1 := now.Add(halfLife)
	score1 := rm.GetScoreAt(peerB, t1)
	if math.Abs(score1-2.0) >= 1e-6 {
		t.Fatalf("Expected decayed score 2.0 after 1 half-life, got %f", score1)
	}

	// After 2 half-lives (20 minutes), score should be 1.0
	t2 := now.Add(2 * halfLife)
	score2 := rm.GetScoreAt(peerB, t2)
	if math.Abs(score2-1.0) >= 1e-6 {
		t.Fatalf("Expected decayed score 1.0 after 2 half-lives, got %f", score2)
	}
}

func TestReputationManager_SuccessReset(t *testing.T) {
	cfg := scheduler.ReputationConfig{
		BanThreshold:  3.0,
		BanDuration:   5 * time.Minute,
		FaultWeight:   1.0,
		DecayHalfLife: 10 * time.Minute,
	}
	rm := scheduler.NewReputationManager(cfg)
	peerC := "peer-c"

	rm.RecordFault(peerC)
	rm.RecordFault(peerC)

	pScore := rm.GetPeerScore(peerC)
	if pScore.ConsecutiveErrors != 2 {
		t.Fatalf("Expected 2 consecutive errors, got %d", pScore.ConsecutiveErrors)
	}

	rm.RecordSuccess(peerC)

	pScore = rm.GetPeerScore(peerC)
	if pScore.ConsecutiveErrors != 0 {
		t.Fatalf("Expected consecutive errors to reset to 0 after success, got %d", pScore.ConsecutiveErrors)
	}

	// Single fault after success: score becomes 1.0 + 1.0 = 2.0 (< 3.0 threshold), not banned
	rm.RecordFault(peerC)
	if rm.IsBanned(peerC) {
		t.Fatalf("Peer should not be banned yet after 1 fault post-success")
	}

	// Second fault brings score to 3.0, triggering ban
	rm.RecordFault(peerC)
	if !rm.IsBanned(peerC) {
		t.Fatalf("Peer should be banned now that threshold is reached again")
	}
}

func TestReputationManager_PeerIDAndUnban(t *testing.T) {
	rm := scheduler.NewReputationManager()
	pID := peer.ID("test-peer-id-12345")

	rm.RecordFaultForPeer(pID)
	rm.RecordFaultForPeer(pID)
	rm.RecordFaultForPeer(pID)

	if !rm.IsBannedForPeer(pID) {
		t.Fatalf("Peer %s should be banned", pID)
	}

	rm.Unban(pID.String())
	if rm.IsBannedForPeer(pID) {
		t.Fatalf("Peer %s should be unbanned after Unban", pID)
	}
}

func TestReputationManager_ConcurrentAccess(t *testing.T) {
	rm := scheduler.NewReputationManager()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := fmt.Sprintf("peer-%d", id%5)
			for j := 0; j < 50; j++ {
				rm.RecordFault(p)
				_ = rm.IsBanned(p)
				_ = rm.GetScore(p)
				rm.RecordSuccess(p)
			}
		}(i)
	}

	wg.Wait()
}
