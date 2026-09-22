package reputation_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/reputation"
)

func generateTestPeerID(id int) peer.ID {
	return peer.ID(fmt.Sprintf("peer-%d", id))
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		err      error
		expected reputation.FailureType
	}{
		{errors.New("sha256 hash mismatch: chunk corrupted"), reputation.FailureCorruptData},
		{errors.New("checksum error"), reputation.FailureCorruptData},
		{context.DeadlineExceeded, reputation.FailureTimeout},
		{errors.New("stream timeout"), reputation.FailureTimeout},
		{errors.New("bad request: chunk not found"), reputation.FailureBadRequest},
		{errors.New("stream reset by peer"), reputation.FailureNetworkError},
		{errors.New("unknown failure"), reputation.FailureGeneric},
	}

	for _, tt := range tests {
		got := reputation.ClassifyError(tt.err)
		if got != tt.expected {
			t.Errorf("ClassifyError(%v) = %v, expected %v", tt.err, got, tt.expected)
		}
	}
}

func TestScoreAndBanning(t *testing.T) {
	cfg := reputation.DefaultConfig()
	cfg.BanThreshold = 50.0
	cfg.PenaltyCorruptData = 30.0
	cfg.PenaltyTimeout = 25.0
	cfg.SuccessReward = 10.0

	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()
	p := generateTestPeerID(1)

	if sm.IsBanned(ctx, p) {
		t.Fatal("New peer should not be banned initially")
	}

	// 1. Timeout failure (+25 score)
	score, banned := sm.RecordFailure(ctx, p, reputation.FailureTimeout)
	if math.Abs(score-25.0) > 0.5 || banned {
		t.Fatalf("Expected score ~25.0, banned false; got score %.1f, banned %t", score, banned)
	}

	// 2. Corrupt data (+30 score -> total 55 >= 50)
	score, banned = sm.RecordFailure(ctx, p, reputation.FailureCorruptData)
	if math.Abs(score-55.0) > 0.5 || !banned {
		t.Fatalf("Expected score ~55.0, banned true; got score %.1f, banned %t", score, banned)
	}

	if !sm.IsBanned(ctx, p) {
		t.Fatal("IsBanned should return true")
	}

	// 3. Successes reward (-10 x 2 = -20 -> score 35 < 50)
	sm.RecordSuccess(ctx, p)
	score = sm.RecordSuccess(ctx, p)
	if math.Abs(score-35.0) > 0.5 {
		t.Fatalf("Expected score ~35.0 after successes, got %.1f", score)
	}

	if sm.IsBanned(ctx, p) {
		t.Fatal("IsBanned should return false after score recovered below threshold")
	}
}

func TestScoreDecay(t *testing.T) {
	cfg := reputation.DefaultConfig()
	cfg.BanThreshold = 100.0
	cfg.HalfLife = 50 * time.Millisecond
	cfg.PenaltyCorruptData = 80.0

	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()
	p := generateTestPeerID(2)

	score, _ := sm.RecordFailure(ctx, p, reputation.FailureCorruptData)
	if math.Abs(score-80.0) > 0.5 {
		t.Fatalf("Expected initial score ~80.0, got %.1f", score)
	}

	// Wait 1 half life (~50ms)
	time.Sleep(60 * time.Millisecond)

	decayedScore := sm.GetScore(ctx, p)
	if decayedScore >= score || decayedScore > 45.0 {
		t.Fatalf("Expected score to decay below 45.0 after half life, got %.1f", decayedScore)
	}
}

func TestResourceBoundsAndEviction(t *testing.T) {
	maxPeers := 100
	cfg := reputation.DefaultConfig()
	cfg.MaxTrackedPeers = maxPeers

	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()

	// Insert 1000 peers
	for i := 0; i < 1000; i++ {
		p := generateTestPeerID(i)
		sm.RecordFailure(ctx, p, reputation.FailureGeneric)
	}

	if count := sm.TrackedCount(); count > maxPeers {
		t.Fatalf("Tracked peer count %d exceeded MaxTrackedPeers limit %d", count, maxPeers)
	}
}

func TestMemoryFootprintUnderStress(t *testing.T) {
	maxPeers := 1000
	cfg := reputation.DefaultConfig()
	cfg.MaxTrackedPeers = maxPeers

	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Simulate 100,000 requests from random peers
	for i := 0; i < 100000; i++ {
		p := generateTestPeerID(i)
		if i%2 == 0 {
			sm.RecordFailure(ctx, p, reputation.FailureCorruptData)
		} else {
			sm.RecordSuccess(ctx, p)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	if count := sm.TrackedCount(); count > maxPeers {
		t.Fatalf("Tracked peer count %d exceeded maxPeers %d under stress", count, maxPeers)
	}

	heapDiff := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	t.Logf("Memory heap diff after 100k requests: %d bytes (tracked peers: %d)", heapDiff, sm.TrackedCount())
}

func TestConcurrentAccessAndRaceConditions(t *testing.T) {
	cfg := reputation.DefaultConfig()
	cfg.MaxTrackedPeers = 50
	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()

	var wg sync.WaitGroup
	workers := 10
	iterations := 1000

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				peerIdx := (workerID*10 + i) % 100
				p := generateTestPeerID(peerIdx)

				switch i % 4 {
				case 0:
					sm.RecordFailure(ctx, p, reputation.FailureCorruptData)
				case 1:
					sm.RecordSuccess(ctx, p)
				case 2:
					sm.IsBanned(ctx, p)
				case 3:
					sm.GetScore(ctx, p)
				}
			}
		}(w)
	}

	wg.Wait()
}

func TestContextCancellation(t *testing.T) {
	cfg := reputation.DefaultConfig()
	sm := reputation.NewScoreManager(cfg)
	p := generateTestPeerID(10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	score, banned := sm.RecordFailure(ctx, p, reputation.FailureCorruptData)
	if score != 0 || banned {
		t.Fatalf("Expected 0 score and false banned for canceled context, got %.1f, %t", score, banned)
	}

	if sm.IsBanned(ctx, p) {
		t.Fatal("IsBanned should return false when context is canceled")
	}
}

func TestFilterPeers(t *testing.T) {
	cfg := reputation.DefaultConfig()
	cfg.BanThreshold = 50.0
	cfg.PenaltyCorruptData = 60.0

	sm := reputation.NewScoreManager(cfg)
	ctx := context.Background()

	p1 := generateTestPeerID(1) // Honest
	p2 := generateTestPeerID(2) // Malicious
	p3 := generateTestPeerID(3) // Honest

	sm.RecordFailure(ctx, p2, reputation.FailureCorruptData) // Banning p2

	allPeers := []peer.ID{p1, p2, p3}
	unbanned := sm.FilterPeers(ctx, allPeers)

	if len(unbanned) != 2 {
		t.Fatalf("Expected 2 unbanned peers, got %d", len(unbanned))
	}
	for _, p := range unbanned {
		if p == p2 {
			t.Fatalf("Banned peer %s was not filtered out", p2)
		}
	}
}
