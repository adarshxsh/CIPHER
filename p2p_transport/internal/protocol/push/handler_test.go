package push_test

import (
	"bytes"
	"context"
	"testing"

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

func TestPushHandler_RejectsTamperedManifest(t *testing.T) {
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

	eng1 := createTestEngine(t)
	push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)

	ctx := context.Background()

	// Ingest valid file on client host to generate valid manifest
	eng2 := createTestEngine(t)
	data := []byte("hello push protocol test payload data")
	m, err := eng2.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Create Push Client
	client, err := push.NewClient(ctx, transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("Failed to create push client: %v", err)
	}
	defer client.Close()

	// 1. Send valid manifest -> should succeed
	err = client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, mBytes)
	if err != nil {
		t.Fatalf("SendManifest failed for valid manifest: %v", err)
	}

	// 2. Send tampered manifest -> modify chunk IDs in mBytes but keep claimed ContentID
	tamperedM := *m
	var fakeChunkID core.ChunkID
	fakeChunkID[0] = 0xEE
	tamperedM.ChunkIDs = []core.ChunkID{fakeChunkID}
	tamperedBytes, err := tamperedM.Serialize()
	if err != nil {
		t.Fatalf("Serialize tampered manifest failed: %v", err)
	}

	err = client.SendManifest(ctx, m.Descriptor.ID, m.ChunkIDs, tamperedBytes)
	if err == nil {
		t.Fatalf("expected error for tampered push manifest, got nil")
	}
}
