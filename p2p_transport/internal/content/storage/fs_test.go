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

func TestFSStorage_PutAndGetChunk(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	chunkID := core.ChunkID{1, 2, 3}
	data := []byte("hello storage world")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(data)),
			CipherSize: uint32(len(data)),
		},
		Data: data,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("failed to put chunk: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Fatalf("expected chunk to exist, got has=%v, err=%v", has, err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("failed to get chunk: %v", err)
	}

	if string(retrieved.Data) != string(data) {
		t.Fatalf("expected data %q, got %q", data, retrieved.Data)
	}
}

func TestFSStorage_GetChunk_HeaderExceedsMaxChunkSize(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	chunkID := core.ChunkID{4, 5, 6}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// Create file with header CipherSize > MaxChunkSize
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: uint32(MaxChunkSize + 100),
	}

	f, err := os.Create(path)
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
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Fatalf("expected ErrChunkTooLarge, got %v", err)
	}
}

func TestFSStorage_GetChunk_DataExceedsHeaderLimit(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	chunkID := core.ChunkID{7, 8, 9}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// Header specifies 10 bytes, but file has 50 bytes of payload
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 10,
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	extraData := make([]byte, 50)
	if _, err := f.Write(extraData); err != nil {
		f.Close()
		t.Fatalf("failed to write data: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error when file data exceeds header size limit, got nil")
	}
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Fatalf("expected ErrChunkTooLarge, got %v", err)
	}
}

func TestFSStorage_GetChunk_TruncatedFile(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	chunkID := core.ChunkID{10, 11, 12}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// Header specifies 100 bytes, but file only has 10 bytes of payload
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 100,
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	shortData := make([]byte, 10)
	if _, err := f.Write(shortData); err != nil {
		f.Close()
		t.Fatalf("failed to write short data: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error on truncated chunk file, got nil")
	}
}

func TestFSStorage_GetManifestBytes_ValidAndTooLarge(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	contentID := core.ContentID{1, 3, 5, 7}

	// 1. Valid manifest
	manifestData := []byte(`{"descriptor":{"id":"01030507"}}`)
	if err := store.PutManifestBytes(ctx, contentID, manifestData); err != nil {
		t.Fatalf("failed to put manifest: %v", err)
	}

	retrieved, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}
	if string(retrieved) != string(manifestData) {
		t.Fatalf("expected manifest %q, got %q", manifestData, retrieved)
	}

	// 2. Oversized manifest file
	oversizedContentID := core.ContentID{2, 4, 6, 8}
	path := store.manifestPath(oversizedContentID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	// Truncate file to MaxManifestSize + 10 bytes without allocating memory
	if err := f.Truncate(MaxManifestSize + 10); err != nil {
		f.Close()
		t.Fatalf("truncate failed: %v", err)
	}
	f.Close()

	_, err = store.GetManifestBytes(ctx, oversizedContentID)
	if err == nil {
		t.Fatal("expected ErrManifestTooLarge, got nil")
	}
	if !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got %v", err)
	}
}
