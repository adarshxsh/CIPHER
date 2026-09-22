package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func TestPeerReputationTracker_Blacklisting(t *testing.T) {
	tracker := scheduler.NewPeerReputationTracker()
	p := mockPeerID("peer1")

	if tracker.IsBlacklisted(p) {
		t.Fatal("Expected peer not to be blacklisted initially")
	}

	// 1st failure
	tracker.RecordFailure(p)
	if tracker.IsBlacklisted(p) {
		t.Fatal("Peer should not be blacklisted after 1 failure")
	}
	if tracker.GetConsecutiveFailures(p) != 1 {
		t.Fatalf("Expected 1 consecutive failure, got %d", tracker.GetConsecutiveFailures(p))
	}

	// 2nd failure
	tracker.RecordFailure(p)
	if tracker.IsBlacklisted(p) {
		t.Fatal("Peer should not be blacklisted after 2 failures")
	}

	// 3rd failure -> blacklisted
	tracker.RecordFailure(p)
	if !tracker.IsBlacklisted(p) {
		t.Fatal("Peer should be blacklisted after 3 failures")
	}
}

func TestPeerReputationTracker_SuccessReset(t *testing.T) {
	tracker := scheduler.NewPeerReputationTracker()
	p := mockPeerID("peer2")

	tracker.RecordFailure(p)
	tracker.RecordFailure(p)
	if tracker.GetConsecutiveFailures(p) != 2 {
		t.Fatalf("Expected 2 consecutive failures, got %d", tracker.GetConsecutiveFailures(p))
	}

	// Success resets consecutive failures
	tracker.RecordSuccess(p)
	if tracker.GetConsecutiveFailures(p) != 0 {
		t.Fatalf("Expected 0 consecutive failures after success, got %d", tracker.GetConsecutiveFailures(p))
	}

	// Another failure is now count 1, so not blacklisted
	tracker.RecordFailure(p)
	if tracker.IsBlacklisted(p) {
		t.Fatal("Peer should not be blacklisted after success reset")
	}
}

func TestPeerReputationTracker_ExponentialBackoff(t *testing.T) {
	tracker := scheduler.NewPeerReputationTracker()
	tracker.SetBackoffParams(100*time.Millisecond, 2*time.Second)
	p := mockPeerID("peer3")

	if backoff := tracker.GetBackoff(p); backoff != 0 {
		t.Fatalf("Expected 0 backoff initially, got %v", backoff)
	}

	tracker.RecordFailure(p) // 1 failure -> initialBackoff (100ms)
	if backoff := tracker.GetBackoff(p); backoff != 100*time.Millisecond {
		t.Fatalf("Expected 100ms backoff, got %v", backoff)
	}

	tracker.RecordFailure(p) // 2 failures -> 200ms
	if backoff := tracker.GetBackoff(p); backoff != 200*time.Millisecond {
		t.Fatalf("Expected 200ms backoff, got %v", backoff)
	}
}

func TestPeerReputationTracker_Decay(t *testing.T) {
	tracker := scheduler.NewPeerReputationTracker()
	tracker.SetDecayInterval(50 * time.Millisecond)
	p := mockPeerID("peer4")

	// Blacklist peer with 3 failures
	tracker.RecordFailure(p)
	tracker.RecordFailure(p)
	tracker.RecordFailure(p)

	if !tracker.IsBlacklisted(p) {
		t.Fatal("Expected peer to be blacklisted")
	}

	// Wait for decay interval to elapse
	time.Sleep(60 * time.Millisecond)

	// Decay should reduce consecutive failures to 2, unblacklisting the peer
	if tracker.IsBlacklisted(p) {
		t.Fatal("Expected peer to be unblacklisted after decay interval")
	}
	if tracker.GetConsecutiveFailures(p) != 2 {
		t.Fatalf("Expected consecutive failures to decay to 2, got %d", tracker.GetConsecutiveFailures(p))
	}
}

func TestPeerReputationTracker_Concurrency(t *testing.T) {
	tracker := scheduler.NewPeerReputationTracker()
	var wg sync.WaitGroup

	numGoroutines := 50
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := mockPeerID(fmt.Sprintf("peer_%d", id%5))
			for j := 0; j < 20; j++ {
				tracker.RecordFailure(p)
				_ = tracker.IsBlacklisted(p)
				_ = tracker.GetBackoff(p)
				_ = tracker.GetScore(p)
				tracker.RecordSuccess(p)
			}
		}(i)
	}
	wg.Wait()
}

func TestScheduler_ByzantinePeerIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mocknet := mocknet.New()

	hGood, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hBad, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hClient, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	engGood := createTestEngine(t)
	engClient := createTestEngine(t)

	// Set up valid stream handler on Good peer
	chunk.NewStreamHandler(hGood, engGood)

	// Set up malicious stream handler on Bad peer that returns corrupted chunk data
	setupCorruptStreamHandler(hBad)

	// Good peer ingests a file with 16 chunks so Bad peer has opportunity to fail 3 times
	dataSize := 16 * 256 * 1024 // 4MB / 256KB = 16 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tClient := transport.NewTransport(hClient)

	// Setup tasks and sources for Scheduler
	var tasks []scheduler.ChunkTask
	for i, cID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  cID,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
		{PeerID: hGood.ID()},
	}

	sched := scheduler.NewScheduler(tClient, engClient, 3)
	// Minimal backoff for test speed
	sched.Tracker.SetBackoffParams(1*time.Millisecond, 10*time.Millisecond)

	completions := make(chan scheduler.WorkerResult, len(tasks))

	runErrCh := make(chan error, 1)
	go func() {
		err := sched.Run(ctx, tasks, sources, completions)
		close(completions)
		runErrCh <- err
	}()

	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
		}
	}

	if err := <-runErrCh; err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	if completedCount != len(m.ChunkIDs) {
		t.Fatalf("Expected %d completed chunks, got %d", len(m.ChunkIDs), completedCount)
	}

	// Verify Bad peer was blacklisted
	if !sched.Tracker.IsBlacklisted(hBad.ID()) {
		t.Fatalf("Expected bad peer %s to be blacklisted after 3 failures", hBad.ID())
	}

	// Verify Good peer was NOT blacklisted
	if sched.Tracker.IsBlacklisted(hGood.ID()) {
		t.Fatalf("Expected good peer %s NOT to be blacklisted", hGood.ID())
	}

	// Verify Client Engine has all chunks
	for _, cID := range m.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, cID); err != nil {
			t.Errorf("Client engine missing chunk %x: %v", cID, err)
		}
	}
}

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func mockPeerID(s string) peer.ID {
	return peer.ID(s)
}

func setupCorruptStreamHandler(h host.Host) {
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		for {
			msg, err := chunk.ReadMessage(s)
			if err != nil {
				return
			}
			if msg.Type == chunk.MsgRequestChunk {
				chunkID, err := chunk.ParseRequestChunk(msg.Payload)
				if err != nil {
					return
				}
				// Return corrupted payload (junk bytes)
				corruptChunk := &core.Chunk{
					Header: core.ChunkHeader{ID: chunkID, PlainSize: 100, CipherSize: 100},
					Data:   bytes.Repeat([]byte("BAD_DATA_CORRUPT"), 10),
				}
				resp, err := chunk.BuildChunk(corruptChunk)
				if err != nil {
					return
				}
				if err := chunk.WriteMessage(s, resp); err != nil {
					return
				}
			}
		}
	})
}
