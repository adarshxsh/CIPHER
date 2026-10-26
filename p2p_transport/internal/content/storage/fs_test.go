package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_PutAndGetChunk_Valid(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0x12
	chunkID[31] = 0x34

	payload := []byte("hello world chunk data")
	chk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload)),
			CipherSize: 0,
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Fatalf("HasChunk failed: has=%v, err=%v", has, err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if string(retrieved.Data) != string(payload) {
		t.Fatalf("expected payload %q, got %q", payload, retrieved.Data)
	}
}

func TestFSStorage_GetChunk_TruncatedHeader(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xAB

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write less than 66 bytes header
	if err := os.WriteFile(path, []byte("short header"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for truncated header, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedPayload(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xBC

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Header claiming CipherSize > MaxCiphertextSize
	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: uint32(core.MaxCiphertextSize + 100),
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	// Write dummy data matching the claimed size so file size isn't smaller than expected
	dummy := make([]byte, core.MaxCiphertextSize+100)
	f.Write(dummy)
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized chunk payload, got nil")
	}
}

func TestFSStorage_GetChunk_TruncatedPayload(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xCD

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: 100,
	}
	binary.Write(f, binary.LittleEndian, &hdr)
	f.Write(make([]byte, 50)) // Only 50 bytes instead of 100
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for truncated chunk payload, got nil")
	}
}

func TestFSStorage_GetChunk_TrailingGarbage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xDE

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: 50,
	}
	binary.Write(f, binary.LittleEndian, &hdr)
	f.Write(make([]byte, 60)) // 60 bytes instead of 50 (trailing garbage)
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for trailing garbage in chunk file, got nil")
	}
}

func TestFSStorage_PutAndGetManifest_Valid(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var contentID core.ContentID
	contentID[0] = 0x55

	data := []byte(`{"version":1,"id":"test"}`)
	if err := store.PutManifestBytes(ctx, contentID, data); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	retrieved, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("GetManifestBytes failed: %v", err)
	}

	if string(retrieved) != string(data) {
		t.Fatalf("expected manifest %q, got %q", data, retrieved)
	}
}

func TestFSStorage_GetManifestBytes_Oversized(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var contentID core.ContentID
	contentID[0] = 0x66

	path := store.manifestPath(contentID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Create oversized manifest file exceeding MaxManifestSize
	oversizedData := make([]byte, core.MaxManifestSize+1024)
	if err := os.WriteFile(path, oversizedData, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err = store.GetManifestBytes(ctx, contentID)
	if err == nil {
		t.Fatalf("expected error for oversized manifest file, got nil")
	}
}

func TestFSStorage_GetManifestBytes_NotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fsstorage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store := NewFSStore(tempDir)
	ctx := context.Background()

	var contentID core.ContentID
	contentID[0] = 0x77

	_, err = store.GetManifestBytes(ctx, contentID)
	if err == nil {
		t.Fatalf("expected error for non-existent manifest, got nil")
	}
}
