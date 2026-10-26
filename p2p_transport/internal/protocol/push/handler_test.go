package push_test

import (
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

func TestPushHandler_ManifestDigestValidation(t *testing.T) {
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

	// Server on h1
	_ = push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)

	t1 := transport.NewTransport(h2)

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{{0x01}},
		Crypto: manifest.CryptoDescriptor{
			Algorithm: "ChaCha20-Poly1305",
		},
	}

	contentID, err := m.ComputeDigest()
	if err != nil {
		t.Fatalf("ComputeDigest failed: %v", err)
	}
	m.Descriptor.ID = contentID

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	client, err := push.NewClient(context.Background(), t1, h1.ID())
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Push valid manifest
	err = client.SendManifest(context.Background(), contentID, m.ChunkIDs, mBytes)
	if err != nil {
		t.Fatalf("SendManifest failed for valid manifest: %v", err)
	}

	// Push tampered manifest (e.g. altered size field in manifest payload)
	mTampered := *m
	mTampered.Descriptor.Size = 9999
	mTamperedBytes, _ := mTampered.Serialize()

	err = client.SendManifest(context.Background(), contentID, m.ChunkIDs, mTamperedBytes)
	if err == nil {
		t.Fatalf("expected error when pushing tampered manifest payload, got nil")
	}
}
