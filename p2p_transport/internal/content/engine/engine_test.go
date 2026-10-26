package engine

import (
	"bytes"
	"context"
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

	// Reassemble
	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("failed to reassemble: %v", err)
	}

	if !bytes.Equal(originalData, outBuf.Bytes()) {
		t.Errorf("reassembled data does not match original data")
	}
}

func TestContentEngine_CanonicalContentIDDerivation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-engine-digest-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := NewLocalKeyProvider()

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)
	eng := NewContentEngine(config, enc, dig, store, store, keys, store)

	ctx := context.Background()
	data := []byte("hello world canonical manifest digest test")

	m, err := eng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	computedHash := dig.Sum(mBytes)
	var expectedID core.ContentID
	copy(expectedID[:], computedHash[:])

	if m.Descriptor.ID != expectedID {
		t.Fatalf("ContentID mismatch: got %x, expected %x", m.Descriptor.ID, expectedID)
	}

	// Verify key and manifest are stored under derived ContentID
	_, err = keys.Get(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Key not stored under derived ContentID: %v", err)
	}

	retrievedManifestBytes, err := eng.GetManifestBytes(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Manifest bytes not stored under derived ContentID: %v", err)
	}

	mDeserialized, err := manifest.Deserialize(retrievedManifestBytes)
	if err != nil {
		t.Fatalf("Failed to deserialize retrieved manifest bytes: %v", err)
	}

	if mDeserialized.Descriptor.ID != m.Descriptor.ID {
		t.Fatalf("Deserialized manifest ID mismatch: got %x, expected %x", mDeserialized.Descriptor.ID, m.Descriptor.ID)
	}
}
