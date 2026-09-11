package retrieval_test

import (
	"bytes"
	"context"
	"testing"

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
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestResolveManifest_SuccessAndFailure(t *testing.T) {
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

	t1 := transport.NewTransport(h1)
	t2 := transport.NewTransport(h2)
	_ = t1

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()

	// Ingest payload into eng1
	payload := []byte("hello manifest resolution test")
	m1, err := eng1.Ingest(ctx, bytes.NewReader(payload), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, err := m1.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	if err := eng1.PutManifestBytes(ctx, m1.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	// 1. Resolve from valid provider h1
	resolved, err := retrieval.ResolveManifest(ctx, m1.Descriptor.ID, nil, t2, eng2, []peer.ID{h1.ID()})
	if err != nil {
		t.Fatalf("ResolveManifest failed: %v", err)
	}
	if resolved.Descriptor.ID != m1.Descriptor.ID {
		t.Errorf("expected content ID %x, got %x", m1.Descriptor.ID, resolved.Descriptor.ID)
	}

	// 2. Resolve non-existent content ID from provider h1 (should fail gracefully)
	var missingID core.ContentID
	missingID[0] = 0x99
	_, err = retrieval.ResolveManifest(ctx, missingID, nil, t2, eng2, []peer.ID{h1.ID()})
	if err == nil {
		t.Fatal("expected error resolving missing manifest, got nil")
	}
}
