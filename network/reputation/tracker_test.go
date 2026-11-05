package reputation

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestReputationTracker_FaultScoringAndQuarantine(t *testing.T) {
	peer1 := peer.ID("peer-1")

	cfg := Config{
		QuarantineThreshold: 10.0,
		HashMismatchPenalty: 10.0,
		ProtocolPenalty:     5.0,
		TransportPenalty:    2.5,
		DecayInterval:       1 * time.Minute,
		DecayFactor:         0.5,
	}

	tr := NewTracker(cfg)

	if tr.IsQuarantined(peer1) {
		t.Fatal("Expected peer1 to not be quarantined initially")
	}

	// Record transport error (score = 2.5)
	tr.RecordFault(peer1, FaultTransport)
	if tr.GetScore(peer1) != 2.5 {
		t.Fatalf("Expected score 2.5, got %f", tr.GetScore(peer1))
	}
	if tr.IsQuarantined(peer1) {
		t.Fatal("Expected peer1 to not be quarantined after single transport error")
	}

	// Record protocol error (score = 7.5)
	tr.RecordFault(peer1, FaultProtocol)
	if tr.GetScore(peer1) != 7.5 {
		t.Fatalf("Expected score 7.5, got %f", tr.GetScore(peer1))
	}
	if tr.IsQuarantined(peer1) {
		t.Fatal("Expected peer1 to not be quarantined after protocol error")
	}

	// Record transport error (score = 10.0 -> quarantined)
	newlyQ := tr.RecordFault(peer1, FaultTransport)
	if !newlyQ {
		t.Fatal("Expected RecordFault to return true when peer is newly quarantined")
	}
	if !tr.IsQuarantined(peer1) {
		t.Fatal("Expected peer1 to be quarantined")
	}

	metrics := tr.GetMetrics(peer1)
	if metrics.TransportErrors != 2 || metrics.ProtocolErrors != 1 || metrics.HashFailures != 0 {
		t.Fatalf("Unexpected metrics: %+v", metrics)
	}
}

func TestReputationTracker_HashMismatchImmediateQuarantine(t *testing.T) {
	peer2 := peer.ID("peer-2")
	tr := NewTracker()

	tr.RecordFault(peer2, FaultHashMismatch)
	if !tr.IsQuarantined(peer2) {
		t.Fatal("Expected hash mismatch to trigger immediate quarantine under default config")
	}

	m := tr.GetMetrics(peer2)
	if m.HashFailures != 1 {
		t.Fatalf("Expected 1 hash failure, got %d", m.HashFailures)
	}
}

func TestReputationTracker_DecayAndReset(t *testing.T) {
	peer3 := peer.ID("peer-3")
	cfg := Config{
		QuarantineThreshold: 10.0,
		HashMismatchPenalty: 5.0,
		ProtocolPenalty:     4.0,
		TransportPenalty:    2.0,
		DecayInterval:       1 * time.Minute,
		DecayFactor:         0.5,
	}
	tr := NewTracker(cfg)

	tr.RecordFault(peer3, FaultProtocol) // 4.0
	if tr.GetScore(peer3) != 4.0 {
		t.Fatalf("Expected score 4.0, got %f", tr.GetScore(peer3))
	}

	tr.ApplyDecay() // 4.0 * 0.5 = 2.0
	if tr.GetScore(peer3) != 2.0 {
		t.Fatalf("Expected score 2.0 after decay, got %f", tr.GetScore(peer3))
	}

	tr.Unquarantine(peer3)
	if tr.GetScore(peer3) != 0 {
		t.Fatalf("Expected score 0 after reset, got %f", tr.GetScore(peer3))
	}
}

func TestReputationTracker_ConcurrentSafety(t *testing.T) {
	tr := NewTracker()
	var wg sync.WaitGroup

	peers := []peer.ID{peer.ID("p1"), peer.ID("p2"), peer.ID("p3")}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := peers[idx%len(peers)]
			tr.RecordFault(p, FaultTransport)
			tr.RecordSuccess(p)
			_ = tr.IsQuarantined(p)
			_ = tr.GetMetrics(p)
		}(i)
	}

	wg.Wait()
}
