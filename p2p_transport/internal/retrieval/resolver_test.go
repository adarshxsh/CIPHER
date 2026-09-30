package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/retrieval"
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

func setupMockNetwork(t testing.TB, numPeers int) ([]host.Host, mocknet.Mocknet) {
	mn := mocknet.New()
	hosts := make([]host.Host, numPeers)
	for i := 0; i < numPeers; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts, mn
}

func TestResolveManifest_SingleProvider(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 2)
	h1, h2 := hosts[0], hosts[1]

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t2 := transport.NewTransport(h2)
	providers := []peer.ID{h1.ID()}

	resolved, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t2, eng2, providers)
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}

	if resolved.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("Expected manifest ID %x, got %x", m.Descriptor.ID, resolved.Descriptor.ID)
	}
}

func TestResolveManifest_ParallelFanOut_MultipleProviders(t *testing.T) {
	// Setup 4 peers: client peer (0) and 3 provider peers (1, 2, 3)
	hosts, _ := setupMockNetwork(t, 4)
	clientHost := hosts[0]

	engClient := createTestEngine(t)
	chunk.NewStreamHandler(clientHost, engClient)

	// Provider 1 does NOT have the manifest
	engP1 := createTestEngine(t)
	chunk.NewStreamHandler(hosts[1], engP1)

	// Provider 2 HAS the manifest
	engP2 := createTestEngine(t)
	chunk.NewStreamHandler(hosts[2], engP2)

	// Provider 3 does NOT have the manifest
	engP3 := createTestEngine(t)
	chunk.NewStreamHandler(hosts[3], engP3)

	ctx := context.Background()
	data := make([]byte, 2048)
	rand.Read(data)

	m, err := engP2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	engP2.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tClient := transport.NewTransport(clientHost)
	providers := []peer.ID{hosts[1].ID(), hosts[2].ID(), hosts[3].ID()}

	resolved, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, tClient, engClient, providers)
	if err != nil {
		t.Fatalf("ResolveManifest failed in parallel fan-out: %v", err)
	}

	if resolved.Descriptor.ID != m.Descriptor.ID {
		t.Errorf("Expected manifest ID %x, got %x", m.Descriptor.ID, resolved.Descriptor.ID)
	}
}

func TestResolveManifest_AllProvidersFail(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 3)
	clientHost := hosts[0]

	tClient := transport.NewTransport(clientHost)
	providers := []peer.ID{hosts[1].ID(), hosts[2].ID()}

	var missingID core.ContentID
	missingID[0] = 0xFF

	_, err := retrieval.ResolveManifest(context.Background(), missingID, nil, tClient, nil, providers)
	if err == nil {
		t.Fatalf("Expected error when all providers fail, got nil")
	}
}

func TestResolveManifest_EmptyProviders(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 1)
	clientHost := hosts[0]
	tClient := transport.NewTransport(clientHost)

	var missingID core.ContentID
	_, err := retrieval.ResolveManifest(context.Background(), missingID, nil, tClient, nil, nil)
	if err == nil {
		t.Fatalf("Expected error for empty providers list, got nil")
	}
}

func TestResolveManifest_ContextTimeout(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 2)
	clientHost := hosts[0]
	tClient := transport.NewTransport(clientHost)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure context expires

	var id core.ContentID
	_, err := retrieval.ResolveManifest(ctx, id, nil, tClient, nil, []peer.ID{hosts[1].ID()})
	if err == nil {
		t.Fatalf("Expected context cancellation error, got nil")
	}
}
