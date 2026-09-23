package push_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h3, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func generateDummyManifest(t testing.TB) (core.ContentID, []core.ChunkID, []byte) {
	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])

	var cid1 core.ChunkID
	_, _ = rand.Read(cid1[:])

	assigned := []core.ChunkID{cid1}

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID: contentID,
		},
		ChunkIDs: assigned,
	}
	manifestData, err := m.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	return contentID, assigned, manifestData
}

func TestMaxSessionsPerPeer(t *testing.T) {
	hProvider, hClientA, hClientB := setupMockNetwork(t)
	engProvider := createTestEngine(t)

	cfg := push.StreamHandlerConfig{
		MaxPendingSessions: 10,
		MaxSessionsPerPeer: 2,
		SessionTTL:         10 * time.Minute,
		SessionIdleTimeout: 10 * time.Minute,
		GCTickerInterval:   1 * time.Minute,
	}

	handler := push.NewStreamHandlerWithConfig(hProvider, engProvider, nil, true, push.AuthPolicyOpen, nil, cfg)
	defer handler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tClientA := transport.NewTransport(hClientA)
	clientA, err := push.NewClient(ctx, tClientA, hProvider.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()

	tClientB := transport.NewTransport(hClientB)
	clientB, err := push.NewClient(ctx, tClientB, hProvider.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()

	// Client A sends session 1
	cID1, assigned1, data1 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID1, assigned1, data1); err != nil {
		t.Fatalf("Client A manifest 1 failed: %v", err)
	}

	// Client A sends session 2
	cID2, assigned2, data2 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID2, assigned2, data2); err != nil {
		t.Fatalf("Client A manifest 2 failed: %v", err)
	}

	if count := handler.PeerSessionCount(hClientA.ID()); count != 2 {
		t.Errorf("Expected peer session count 2 for Client A, got %d", count)
	}

	// Client A sends session 3 -> should fail due to MaxSessionsPerPeer = 2
	cID3, assigned3, data3 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID3, assigned3, data3); err == nil {
		t.Fatalf("Expected Client A manifest 3 to be rejected due to per-peer limit, but it succeeded")
	}

	// Client B sends session 1 -> should succeed because Client B quota is independent
	cIDB1, assignedB1, dataB1 := generateDummyManifest(t)
	if err := clientB.SendManifest(ctx, cIDB1, assignedB1, dataB1); err != nil {
		t.Fatalf("Client B manifest 1 failed: %v", err)
	}

	if count := handler.PeerSessionCount(hClientB.ID()); count != 1 {
		t.Errorf("Expected peer session count 1 for Client B, got %d", count)
	}
}

func TestMaxPendingSessionsWithImmediateEviction(t *testing.T) {
	hProvider, hClientA, hClientB := setupMockNetwork(t)
	engProvider := createTestEngine(t)

	cfg := push.StreamHandlerConfig{
		MaxPendingSessions: 2,
		MaxSessionsPerPeer: 10,
		SessionTTL:         100 * time.Millisecond,
		SessionIdleTimeout: 100 * time.Millisecond,
		GCTickerInterval:   10 * time.Hour, // GC ticker won't fire automatically during test
	}

	handler := push.NewStreamHandlerWithConfig(hProvider, engProvider, nil, true, push.AuthPolicyOpen, nil, cfg)
	defer handler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tClientA := transport.NewTransport(hClientA)
	clientA, err := push.NewClient(ctx, tClientA, hProvider.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()

	tClientB := transport.NewTransport(hClientB)
	clientB, err := push.NewClient(ctx, tClientB, hProvider.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()

	// Fill capacity (2 sessions)
	cID1, assigned1, data1 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID1, assigned1, data1); err != nil {
		t.Fatalf("Manifest 1 failed: %v", err)
	}

	cID2, assigned2, data2 := generateDummyManifest(t)
	if err := clientB.SendManifest(ctx, cID2, assigned2, data2); err != nil {
		t.Fatalf("Manifest 2 failed: %v", err)
	}

	if handler.PendingSessionCount() != 2 {
		t.Fatalf("Expected 2 pending sessions, got %d", handler.PendingSessionCount())
	}

	// Immediate attempt without waiting -> capacity reached and no sessions expired -> rejected
	cID3, assigned3, data3 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID3, assigned3, data3); err == nil {
		t.Fatalf("Expected manifest 3 to be rejected when capacity reached without expired sessions")
	}

	// Sleep past TTL (100ms)
	time.Sleep(150 * time.Millisecond)

	// Send manifest 4 -> capacity reached, but immediate eviction runs on PUSH_MANIFEST and purges expired sessions!
	cID4, assigned4, data4 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID4, assigned4, data4); err != nil {
		t.Fatalf("Expected manifest 4 to succeed after immediate eviction, got error: %v", err)
	}

	// Confirm eviction happened
	if handler.PendingSessionCount() != 1 {
		t.Errorf("Expected 1 pending session after eviction and new addition, got %d", handler.PendingSessionCount())
	}
}

func TestBackgroundGarbageCollection(t *testing.T) {
	hProvider, hClientA, _ := setupMockNetwork(t)
	engProvider := createTestEngine(t)

	cfg := push.StreamHandlerConfig{
		MaxPendingSessions: 100,
		MaxSessionsPerPeer: 10,
		SessionTTL:         10 * time.Minute,
		SessionIdleTimeout: 100 * time.Millisecond, // Expire idle sessions after 100ms
		GCTickerInterval:   30 * time.Millisecond,  // GC ticker runs every 30ms
	}

	handler := push.NewStreamHandlerWithConfig(hProvider, engProvider, nil, true, push.AuthPolicyOpen, nil, cfg)
	defer handler.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tClientA := transport.NewTransport(hClientA)
	clientA, err := push.NewClient(ctx, tClientA, hProvider.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()

	cID1, assigned1, data1 := generateDummyManifest(t)
	if err := clientA.SendManifest(ctx, cID1, assigned1, data1); err != nil {
		t.Fatalf("Manifest 1 failed: %v", err)
	}

	if handler.PendingSessionCount() != 1 {
		t.Fatalf("Expected 1 pending session, got %d", handler.PendingSessionCount())
	}

	// Wait for idle timeout (100ms) plus ticker interval
	time.Sleep(200 * time.Millisecond)

	if handler.PendingSessionCount() != 0 {
		t.Errorf("Expected 0 pending sessions after background GC, got %d", handler.PendingSessionCount())
	}
	if handler.PeerSessionCount(hClientA.ID()) != 0 {
		t.Errorf("Expected 0 peer sessions for Client A after background GC, got %d", handler.PeerSessionCount(hClientA.ID()))
	}
}

func TestCleanShutdown(t *testing.T) {
	hProvider, _, _ := setupMockNetwork(t)
	engProvider := createTestEngine(t)

	cfg := push.StreamHandlerConfig{
		MaxPendingSessions: 10,
		MaxSessionsPerPeer: 5,
		SessionTTL:         1 * time.Minute,
		SessionIdleTimeout: 1 * time.Minute,
		GCTickerInterval:   10 * time.Millisecond,
	}

	handler := push.NewStreamHandlerWithConfig(hProvider, engProvider, nil, true, push.AuthPolicyOpen, nil, cfg)

	err := handler.Close()
	if err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Calling Stop/Close again should be safe and idempotent
	handler.Stop()
}
