package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

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

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestResolveManifest_DigestVerification(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

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

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	t1 := transport.NewTransport(h2)

	// 1. Resolve legitimate manifest passes
	resolved, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t1, eng2, []peer.ID{h1.ID()})
	if err != nil {
		t.Fatalf("Failed to resolve legitimate manifest: %v", err)
	}
	if resolved.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("Resolved manifest ID mismatch: got %x, expected %x", resolved.Descriptor.ID, m.Descriptor.ID)
	}

	// 2. Tamper with manifest in peer 1's store
	var rawMap map[string]any
	if err := json.Unmarshal(mBytes, &rawMap); err != nil {
		t.Fatalf("Failed to unmarshal mBytes: %v", err)
	}
	rawMap["descriptor"].(map[string]any)["size"] = 999999
	tamperedBytes, err := json.Marshal(rawMap)
	if err != nil {
		t.Fatalf("Failed to marshal tampered manifest: %v", err)
	}

	// Put tampered manifest bytes under original ContentID in server store
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, tamperedBytes)

	// 3. Resolve tampered manifest must be rejected
	_, err = retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t1, eng2, []peer.ID{h1.ID()})
	if err == nil {
		t.Fatalf("Expected error when resolving tampered manifest payload, but resolution succeeded")
	}
}
