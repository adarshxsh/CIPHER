package retrieval_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

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

func TestResolveManifest_FallbackOnSpoofedProvider(t *testing.T) {
	mn := mocknet.New()

	// Provider 1 (Spoofed / Malicious)
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	// Provider 2 (Legitimate Provider)
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	// Client
	h3, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	eng3 := createTestEngine(t)

	// Ingest file on legitimate provider (Peer 2)
	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng2.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// On Peer 1 (Malicious), ingest a different content or corrupted manifest for same ContentID
	badManifest := *m
	badManifest.Descriptor.Size = 999999 // Tamper size
	badMBytes, _ := badManifest.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, badMBytes)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	providers := []peer.ID{h1.ID(), h2.ID()}

	// Attempt resolution from Client (Peer 3)
	resolvedManifest, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, transport.NewTransport(h3), eng3, providers)
	if err != nil {
		t.Fatalf("Expected ResolveManifest to succeed via fallback, got: %v", err)
	}

	if resolvedManifest.Descriptor.Size != m.Descriptor.Size {
		t.Errorf("Expected size %d from legitimate provider, got %d", m.Descriptor.Size, resolvedManifest.Descriptor.Size)
	}
}
