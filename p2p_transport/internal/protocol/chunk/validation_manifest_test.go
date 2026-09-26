package chunk_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
)

func TestValidateManifestForRequest_RejectsTamperedPayload(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 1024,
		},
		ChunkIDs: []core.ChunkID{
			{1, 1, 1},
			{2, 2, 2},
		},
		MerkleRoot: core.Hash{3, 3, 3},
		WholeHash:  core.Hash{3, 3, 3},
		Crypto: manifest.CryptoDescriptor{
			Algorithm:      "ChaCha20-Poly1305",
			Version:        1,
			ChunkNonceSize: 12,
			KeyID:          "embedded",
		},
	}

	cid, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}
	m.Descriptor.ID = cid

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	msg := chunk.BuildManifest(cid, mBytes)

	// Valid payload should pass
	if err := chunk.ValidateManifestForRequest(cid, msg.Payload); err != nil {
		t.Fatalf("valid manifest failed validation: %v", err)
	}

	// 1. Header mismatch
	var wrongCID core.ContentID
	wrongCID[0] = 0xFF
	wrongHeaderMsg := chunk.BuildManifest(wrongCID, mBytes)
	err = chunk.ValidateManifestForRequest(cid, wrongHeaderMsg.Payload)
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch for wrong header CID, got %v", err)
	}

	// 2. Tampered ChunkID in payload (single-byte edit)
	mTampered := *m
	mTampered.ChunkIDs = []core.ChunkID{
		{1, 1, 1},
		{2, 2, 3}, // Single-byte edit
	}
	tamperedBytes, _ := mTampered.Serialize()
	tamperedMsg := chunk.BuildManifest(cid, tamperedBytes)

	err = chunk.ValidateManifestForRequest(cid, tamperedMsg.Payload)
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch for tampered ChunkID, got %v", err)
	}

	// 3. Tampered Metadata (Size single-byte edit)
	mTamperedMeta := *m
	mTamperedMeta.Descriptor.Size = 1025 // Single-byte edit in size
	tamperedMetaBytes, _ := mTamperedMeta.Serialize()
	tamperedMetaMsg := chunk.BuildManifest(cid, tamperedMetaBytes)

	err = chunk.ValidateManifestForRequest(cid, tamperedMetaMsg.Payload)
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch for tampered metadata, got %v", err)
	}
}

func TestClientResolve_RejectsSpoofedManifest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}

	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	t1 := transport.NewTransport(h1)

	tmpDir, err := os.MkdirTemp("", "client-resolve-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create valid manifest
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 500,
		},
		ChunkIDs: []core.ChunkID{{10, 20, 30}},
	}
	cid, err := m.ComputeContentID()
	if err != nil {
		t.Fatalf("ComputeContentID failed: %v", err)
	}
	m.Descriptor.ID = cid

	// Store tampered bytes under valid cid in engine
	mTampered := *m
	mTampered.ChunkIDs = []core.ChunkID{{10, 20, 99}} // Spoofed single byte
	tamperedBytes, _ := mTampered.Serialize()
	_ = eng.PutManifestBytes(ctx, cid, tamperedBytes)

	// Set up server stream handler on h2
	chunk.NewStreamHandler(h2, eng)

	client, err := chunk.NewClient(ctx, t1, h2.ID(), eng)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	_, err = client.Resolve(ctx, cid)
	if err == nil {
		t.Fatalf("expected Client.Resolve to reject spoofed manifest, but it succeeded")
	}

	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch in Client.Resolve error, got: %v", err)
	}
}
