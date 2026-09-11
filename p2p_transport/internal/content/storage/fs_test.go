package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_Valid(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage_test_*")
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
			Version:   1,
			ID:        chunkID,
			Index:     0,
			Offset:    0,
			PlainSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, originalChunk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if string(retrieved.Data) != string(payload) {
		t.Errorf("GetChunk payload mismatch: got %q, want %q", retrieved.Data, payload)
	}
	if retrieved.Header.PlainSize != uint32(len(payload)) {
		t.Errorf("GetChunk PlainSize mismatch: got %d, want %d", retrieved.Header.PlainSize, len(payload))
	}
}

func TestFSStorage_GetChunk_OversizedPayload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized_chunk_id_123456789012"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	header := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: MaxChunkResponseSize + 100,
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// Truncate file to header + (MaxChunkResponseSize + 100)
	headerSize := int64(binary.Size(&header))
	if err := f.Truncate(headerSize + int64(MaxChunkResponseSize) + 100); err != nil {
		f.Close()
		t.Fatalf("failed to truncate file: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized chunk payload, got nil")
	}
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Errorf("expected ErrChunkTooLarge, got %v", err)
	}
}

func TestFSStorage_GetChunk_HeaderSizeMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("mismatched_chunk_id_12345678901"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Header claims PlainSize = 100, but we write 200 payload bytes
	header := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: 100,
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	dummyPayload := make([]byte, 200)
	if _, err := f.Write(dummyPayload); err != nil {
		f.Close()
		t.Fatalf("failed to write payload: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for header size mismatch, got nil")
	}
	if !errors.Is(err, ErrHeaderSizeMismatch) {
		t.Errorf("expected ErrHeaderSizeMismatch, got %v", err)
	}
}

func TestFSStorage_GetChunk_TruncatedHeader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("truncated_chunk_id_123456789012"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write only 10 bytes (smaller than 66 byte header)
	if err := os.WriteFile(path, []byte("too short"), 0644); err != nil {
		t.Fatalf("failed to write short file: %v", err)
	}

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for truncated chunk file, got nil")
	}
	if !errors.Is(err, ErrInvalidChunkSize) {
		t.Errorf("expected ErrInvalidChunkSize, got %v", err)
	}
}

func TestFSStorage_GetManifestBytes_ValidAndOversized(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	copy(contentID[:], []byte("manifest_content_id_12345678901"))

	validManifest := []byte(`{"version":1,"descriptor":{"size":100}}`)
	if err := store.PutManifestBytes(ctx, contentID, validManifest); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	data, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("GetManifestBytes failed: %v", err)
	}
	if string(data) != string(validManifest) {
		t.Errorf("GetManifestBytes mismatch: got %q, want %q", data, validManifest)
	}

	// Test oversized manifest
	var oversizedContentID core.ContentID
	copy(oversizedContentID[:], []byte("oversized_manifest_id_1234567890"))

	manifestPath := store.manifestPath(oversizedContentID)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(manifestPath)
	if err != nil {
		t.Fatalf("failed to create manifest file: %v", err)
	}
	if err := f.Truncate(int64(MaxManifestSize) + 500); err != nil {
		f.Close()
		t.Fatalf("failed to truncate manifest file: %v", err)
	}
	f.Close()

	_, err = store.GetManifestBytes(ctx, oversizedContentID)
	if err == nil {
		t.Fatalf("expected error for oversized manifest, got nil")
	}
	if !errors.Is(err, ErrManifestTooLarge) {
		t.Errorf("expected ErrManifestTooLarge, got %v", err)
	}
}
