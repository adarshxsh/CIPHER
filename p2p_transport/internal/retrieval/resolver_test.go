package retrieval_test

import (
	"bytes"
	"context"
	"errors"
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
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestResolveManifest_ValidAndTampered(t *testing.T) {
	mocknet := mocknet.New()
	hServer, err := mocknet.GenPeer()
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

	engServer := createTestEngine(t)
	engClient := createTestEngine(t)

	chunk.NewStreamHandler(hServer, engServer)

	ctx := context.Background()
	data := []byte("hello world testing manifest resolution with zero-id digest")
	m, err := engServer.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	engServer.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	tClient := transport.NewTransport(hClient)

	// 1. Resolve valid manifest from server
	resolvedM, err := retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, tClient, engClient, []peer.ID{hServer.ID()})
	if err != nil {
		t.Fatalf("ResolveManifest failed for valid manifest: %v", err)
	}
	if resolvedM.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("Descriptor.ID mismatch: got %x, want %x", resolvedM.Descriptor.ID, m.Descriptor.ID)
	}

	// 2. Overwrite server manifest store with tampered manifest payload
	mTampered := *m
	mTampered.Descriptor.Size = 999999
	mTamperedBytes, err := mTampered.Serialize()
	if err != nil {
		t.Fatalf("Serialize tampered manifest failed: %v", err)
	}
	// Note: We store tampered bytes under original ContentID key on server
	engServer.PutManifestBytes(ctx, m.Descriptor.ID, mTamperedBytes)

	// 3. Attempt to resolve manifest again
	_, err = retrieval.ResolveManifest(ctx, m.Descriptor.ID, nil, tClient, engClient, []peer.ID{hServer.ID()})
	if err == nil {
		t.Fatalf("expected error when resolving tampered manifest payload, got nil")
	}
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch, got %v", err)
	}
}
