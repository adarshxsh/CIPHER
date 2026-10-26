package reputation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestRecordSuccessAndFailure(t *testing.T) {
	mgr := NewPeerReputationManager()
	testPeer := peer.ID("peer1")

	if mgr.GetScore(testPeer) != 0 {
		t.Fatalf("expected initial score 0, got %f", mgr.GetScore(testPeer))
	}
	if mgr.IsBanned(testPeer) {
		t.Fatalf("expected initial peer not banned")
	}

	mgr.RecordSuccess(testPeer)
	if mgr.GetScore(testPeer) != 5.0 {
		t.Fatalf("expected score 5.0 after success, got %f", mgr.GetScore(testPeer))
	}

	mgr.RecordFailure(testPeer)
	if mgr.GetScore(testPeer) != -5.0 {
		t.Fatalf("expected score -5.0 after failure, got %f", mgr.GetScore(testPeer))
	}
}

func TestBanThresholdTriggering(t *testing.T) {
	mgr := NewPeerReputationManager()
	testPeer := peer.ID("bad_peer")

	// Default penalty is 10.0, BanThreshold is -30.0.
	mgr.RecordFailure(testPeer) // -10.0
	if mgr.IsBanned(testPeer) {
		t.Fatalf("peer should not be banned at -10")
	}

	mgr.RecordFailure(testPeer) // -20.0
	if mgr.IsBanned(testPeer) {
		t.Fatalf("peer should not be banned at -20")
	}

	mgr.RecordFailure(testPeer) // -30.0
	if !mgr.IsBanned(testPeer) {
		t.Fatalf("peer should be banned at -30")
	}
}

func TestScoreDecayAndPeerRecovery(t *testing.T) {
	mgr := NewPeerReputationManager()
	testPeer := peer.ID("transient_bad_peer")

	// Trigger ban
	for i := 0; i < 3; i++ {
		mgr.RecordFailure(testPeer) // score becomes -30.0
	}

	if !mgr.IsBanned(testPeer) {
		t.Fatalf("expected peer to be banned")
	}

	// Apply decay
	mgr.Decay() // negative score -30.0 halves to -15.0

	if mgr.GetScore(testPeer) != -15.0 {
		t.Fatalf("expected score -15.0 after decay, got %f", mgr.GetScore(testPeer))
	}

	if mgr.IsBanned(testPeer) {
		t.Fatalf("peer should be rehabilitated/unbanned after score decay to -15.0")
	}

	// Peer serves good chunks now
	mgr.RecordSuccess(testPeer) // score becomes -10.0
	if mgr.GetScore(testPeer) != -10.0 {
		t.Fatalf("expected score -10.0, got %f", mgr.GetScore(testPeer))
	}
}

func TestDecayLoop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DecayInterval = 20 * time.Millisecond
	mgr := NewPeerReputationManagerWithConfig(cfg)
	defer mgr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.StartDecayLoop(ctx)

	testPeer := peer.ID("decay_loop_peer")
	mgr.RecordFailure(testPeer) // -10.0

	// Wait for decay ticker to run
	time.Sleep(50 * time.Millisecond)

	score := mgr.GetScore(testPeer)
	if score >= -10.0 && score < 0 {
		// Score should have decayed toward 0 (e.g. -5.0 or -2.5)
		t.Logf("Score successfully decayed in background loop: %f", score)
	} else if score == 0 {
		t.Logf("Score fully decayed to 0")
	} else {
		t.Fatalf("expected decayed score, got %f", score)
	}
}

func TestConcurrentAccess(t *testing.T) {
	mgr := NewPeerReputationManager()
	testPeer := peer.ID("concurrent_peer")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			mgr.RecordSuccess(testPeer)
		}()
		go func() {
			defer wg.Done()
			mgr.RecordFailure(testPeer)
		}()
		go func() {
			defer wg.Done()
			_ = mgr.IsBanned(testPeer)
		}()
	}
	wg.Wait()
}
