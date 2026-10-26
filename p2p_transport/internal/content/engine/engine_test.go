package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand"
	"os"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestContentEngine_EndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 32 * 1024, // 32KB
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	// Create random data larger than one chunk
	originalData := make([]byte, 100*1024+500) // ~100.5 KB
	rand.Seed(time.Now().UnixNano())
	rand.Read(originalData)

	ctx := context.Background()

	// Ingest
	reader := bytes.NewReader(originalData)
	m, err := eng.Ingest(ctx, reader, manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Verify Manifest
	if m.Descriptor.Size != uint64(len(originalData)) {
		t.Errorf("manifest size %d != expected %d", m.Descriptor.Size, len(originalData))
	}

	expectedChunks := (len(originalData) + int(config.ChunkSize) - 1) / int(config.ChunkSize)
	if len(m.ChunkIDs) != expectedChunks {
		t.Errorf("manifest chunks %d != expected %d", len(m.ChunkIDs), expectedChunks)
	}

	// Verify ContentID is derived from SHA-256 of canonical manifest bytes
	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}
	expectedCID := sha256.Sum256(mBytes)
	if m.Descriptor.ID != expectedCID {
		t.Fatalf("ContentID mismatch: got %x, expected sha256(manifestBytes) %x", m.Descriptor.ID, expectedCID)
	}

	// Reassemble
	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if !bytes.Equal(originalData, outBuf.Bytes()) {
		t.Errorf("reassembled data does not match original data")
	}
}

func TestContentEngine_Ingest_MultihashCID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-multihash-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{
		ChunkSize: 1024,
	}

	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	data := []byte("Cryptographic manifest multihash CID test data")
	ctx := context.Background()

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	computedID := sha256.Sum256(mBytes)
	if m.Descriptor.ID != computedID {
		t.Fatalf("ingested manifest ContentID %x != computed sha256 %x", m.Descriptor.ID, computedID)
	}

	// Verify key was stored under computedID
	key, err := keys.Get(ctx, computedID)
	if err != nil || len(key) != 32 {
		t.Fatalf("key not properly stored under computed ContentID: %v", err)
	}

	// Verify manifest bytes were stored under computedID
	storedBytes, err := store.GetManifestBytes(ctx, computedID)
	if err != nil {
		t.Fatalf("stored manifest bytes not found under computed ContentID: %v", err)
	}
	if !bytes.Equal(storedBytes, mBytes) {
		t.Fatalf("stored manifest bytes do not match expected canonical bytes")
	}
}
