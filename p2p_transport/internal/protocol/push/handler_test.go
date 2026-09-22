package push

import (
	"bytes"
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

func setupMockNetwork(t testing.TB, numPeers int) ([]host.Host, []*transport.Transport) {
	mocknet := mocknet.New()
	hosts := make([]host.Host, numPeers)
	transports := make([]*transport.Transport, numPeers)

	for i := 0; i < numPeers; i++ {
		h, err := mocknet.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
		transports[i] = transport.NewTransport(h)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts, transports
}

func generateDummyManifest(t *testing.T, eng *engine.ContentEngine) (core.ContentID, []core.ChunkID, []byte) {
	ctx := context.Background()
	data := make([]byte, 1024)
	_, _ = rand.Read(data)

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data for manifest: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize manifest: %v", err)
	}

	return m.Descriptor.ID, m.ChunkIDs, mBytes
}

func TestPendingSession_PeerIDAttribution(t *testing.T) {
	hosts, transports := setupMockNetwork(t, 2)
	eng1 := createTestEngine(t)

	handler := NewStreamHandler(hosts[0], eng1, nil, true, AuthPolicyOpen, nil)
	defer handler.Close()

	client, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	contentID, chunkIDs, manifestBytes := generateDummyManifest(t, eng1)

	err = client.SendManifest(context.Background(), contentID, chunkIDs, manifestBytes)
	if err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	handler.sessionsMu.RLock()
	session, exists := handler.sessions[contentID]
	handler.sessionsMu.RUnlock()

	if !exists {
		t.Fatalf("expected pending session for contentID %x, but found none", contentID)
	}

	if session.PeerID != hosts[1].ID() {
		t.Errorf("expected PeerID %s, got %s", hosts[1].ID(), session.PeerID)
	}
}

func TestHandlePushManifest_GlobalSessionCapacity(t *testing.T) {
	hosts, transports := setupMockNetwork(t, 2)
	eng1 := createTestEngine(t)

	handler := NewStreamHandler(hosts[0], eng1, nil, true, AuthPolicyOpen, nil, WithLimits(2, 10))
	defer handler.Close()

	// Send 1st manifest
	c1, chunks1, bytes1 := generateDummyManifest(t, eng1)
	client1, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	if err := client1.SendManifest(context.Background(), c1, chunks1, bytes1); err != nil {
		t.Fatalf("SendManifest 1 failed: %v", err)
	}

	// Send 2nd manifest
	c2, chunks2, bytes2 := generateDummyManifest(t, eng1)
	client2, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	if err := client2.SendManifest(context.Background(), c2, chunks2, bytes2); err != nil {
		t.Fatalf("SendManifest 2 failed: %v", err)
	}

	// Send 3rd manifest (exceeds global capacity 2)
	c3, chunks3, bytes3 := generateDummyManifest(t, eng1)
	client3, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()

	err = client3.SendManifest(context.Background(), c3, chunks3, bytes3)
	if err == nil {
		t.Fatalf("expected SendManifest 3 to be rejected due to global capacity, but got nil")
	}

	handler.sessionsMu.RLock()
	sessionCount := len(handler.sessions)
	handler.sessionsMu.RUnlock()

	if sessionCount != 2 {
		t.Errorf("expected 2 active sessions, got %d", sessionCount)
	}
}

func TestHandlePushManifest_PerPeerSessionCapacity(t *testing.T) {
	hosts, transports := setupMockNetwork(t, 3)
	eng1 := createTestEngine(t)

	handler := NewStreamHandler(hosts[0], eng1, nil, true, AuthPolicyOpen, nil, WithLimits(100, 2))
	defer handler.Close()

	// Peer 1 sends 2 manifests
	for i := 0; i < 2; i++ {
		c, chunks, b := generateDummyManifest(t, eng1)
		client, err := NewClient(context.Background(), transports[1], hosts[0].ID())
		if err != nil {
			t.Fatal(err)
		}
		if err := client.SendManifest(context.Background(), c, chunks, b); err != nil {
			t.Fatalf("Peer 1 SendManifest %d failed: %v", i, err)
		}
		client.Close()
	}

	// Peer 1 sends 3rd manifest -> should fail per-peer limit
	cExtra, chunksExtra, bExtra := generateDummyManifest(t, eng1)
	client1, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Close()

	err = client1.SendManifest(context.Background(), cExtra, chunksExtra, bExtra)
	if err == nil {
		t.Fatalf("expected Peer 1 3rd SendManifest to fail due to per-peer limit, but succeeded")
	}

	// Peer 2 sends 1st manifest -> should succeed
	cP2, chunksP2, bP2 := generateDummyManifest(t, eng1)
	client2, err := NewClient(context.Background(), transports[2], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	if err := client2.SendManifest(context.Background(), cP2, chunksP2, bP2); err != nil {
		t.Fatalf("Peer 2 SendManifest failed: %v", err)
	}
}

func TestStreamHandler_BackgroundJanitorEviction(t *testing.T) {
	hosts, transports := setupMockNetwork(t, 2)
	eng1 := createTestEngine(t)

	handler := NewStreamHandler(hosts[0], eng1, nil, true, AuthPolicyOpen, nil, WithTTL(50*time.Millisecond, 10*time.Millisecond))
	defer handler.Close()

	c, chunks, b := generateDummyManifest(t, eng1)
	client, err := NewClient(context.Background(), transports[1], hosts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.SendManifest(context.Background(), c, chunks, b); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	handler.sessionsMu.RLock()
	_, exists := handler.sessions[c]
	handler.sessionsMu.RUnlock()
	if !exists {
		t.Fatalf("expected pending session, but not found")
	}

	time.Sleep(150 * time.Millisecond)

	handler.sessionsMu.RLock()
	_, existsAfter := handler.sessions[c]
	handler.sessionsMu.RUnlock()

	if existsAfter {
		t.Errorf("expected session to be evicted by janitor, but it still exists")
	}
}

func TestStreamHandler_CloseNoLeaks(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 1)
	eng := createTestEngine(t)

	handler := NewStreamHandler(hosts[0], eng, nil, true, AuthPolicyOpen, nil, WithTTL(15*time.Minute, 5*time.Millisecond))

	if err := handler.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if err := handler.Close(); err != nil {
		t.Fatalf("Second Close returned error: %v", err)
	}
}
