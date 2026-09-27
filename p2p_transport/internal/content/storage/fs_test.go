package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_PutAndGetChunk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("01234567890123456789012345678901"))

	payload := []byte("hello world chunk payload")
	originalChunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload)),
			CipherSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, originalChunk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	retrievedChunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if retrievedChunk.Header.ID != chunkID {
		t.Errorf("expected ChunkID %x, got %x", chunkID, retrievedChunk.Header.ID)
	}
	if !bytes.Equal(retrievedChunk.Data, payload) {
		t.Errorf("expected payload %q, got %q", payload, retrievedChunk.Data)
	}
}

func TestFSStorage_GetChunk_OversizedCipherSize(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-ciphersize-chunk-id---"))

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write a header declaring CipherSize exceeding MaxCiphertextSize
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		Index:      0,
		PlainSize:  100,
		CipherSize: core.MaxCiphertextSize + 100,
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error reading chunk with oversized CipherSize, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedPlainSize(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-plainsize-chunk-id----"))

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		Index:      0,
		PlainSize:  core.MaxCiphertextSize + 500,
		CipherSize: 0,
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error reading chunk with oversized PlainSize, got nil")
	}
}

func TestFSStorage_GetChunk_TrailingBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("trailing-bytes-chunk-id---------"))

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	payload := []byte("exact payload bytes")
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		Index:      0,
		PlainSize:  uint32(len(payload)),
		CipherSize: uint32(len(payload)),
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		t.Fatalf("failed to write payload: %v", err)
	}
	// Append extra unexpected bytes at end
	if _, err := f.Write([]byte("unexpected extra trailing bytes")); err != nil {
		f.Close()
		t.Fatalf("failed to write extra bytes: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error reading chunk with trailing bytes, got nil")
	}
}

func TestFSStorage_GetChunk_TruncatedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("truncated-file-chunk-id---------"))

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		Index:      0,
		PlainSize:  100,
		CipherSize: 100,
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	// Only write 10 bytes instead of declared 100
	if _, err := f.Write([]byte("short data")); err != nil {
		f.Close()
		t.Fatalf("failed to write payload: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error reading truncated chunk file, got nil")
	}
}

func TestFSStorage_PutAndGetManifestBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	copy(contentID[:], []byte("manifest-content-id-------------"))

	manifestData := []byte(`{"version":1,"descriptor":{"id":"test"}}`)
	if err := store.PutManifestBytes(ctx, contentID, manifestData); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	retrieved, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("GetManifestBytes failed: %v", err)
	}

	if !bytes.Equal(retrieved, manifestData) {
		t.Errorf("expected manifest %s, got %s", manifestData, retrieved)
	}
}

func TestFSStorage_GetManifestBytes_Oversized(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	copy(contentID[:], []byte("oversized-manifest-content-id---"))

	manifestPath := store.manifestPath(contentID)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Create a file exceeding MaxManifestSize
	oversizedData := make([]byte, core.MaxManifestSize+100)
	if err := os.WriteFile(manifestPath, oversizedData, 0644); err != nil {
		t.Fatalf("failed to write oversized manifest file: %v", err)
	}

	_, err = store.GetManifestBytes(ctx, contentID)
	if err == nil {
		t.Fatalf("expected error reading oversized manifest file, got nil")
	}
}
