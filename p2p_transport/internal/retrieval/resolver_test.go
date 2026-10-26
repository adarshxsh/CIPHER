package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
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
	config := core.EngineConfig{ChunkSize: 64 * 1024}
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

func TestResolveManifest_ValidAndTampered(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 10*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	t1 := transport.NewTransport(h1)
	t2 := transport.NewTransport(h2)
	_ = t1

	// Test 1: Resolve valid manifest
	resolvedManifest, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t2, eng2, []peer.ID{h1.ID()})
	if err != nil {
		t.Fatalf("ResolveManifest failed for valid manifest: %v", err)
	}

	if resolvedManifest.Descriptor.Size != m.Descriptor.Size {
		t.Fatalf("Resolved manifest size mismatch: %d != %d", resolvedManifest.Descriptor.Size, m.Descriptor.Size)
	}

	// Test 2: Spoofed / Tampered Manifest
	// Provider replaces stored manifest bytes with tampered bytes under m.Descriptor.ID
	tamperedManifest := *m
	tamperedManifest.Descriptor.Size = 999999 // alter manifest content
	tamperedBytes, _ := tamperedManifest.Serialize()

	if err := eng1.PutManifestBytes(ctx, m.Descriptor.ID, tamperedBytes); err != nil {
		t.Fatalf("Failed to put tampered manifest bytes: %v", err)
	}

	// Attempt resolution again - client must detect multihash mismatch and reject tampered manifest
	_, err = retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, t2, eng2, []peer.ID{h1.ID()})
	if err == nil {
		t.Fatalf("Expected ResolveManifest to fail and reject tampered manifest, but it succeeded")
	}
}
