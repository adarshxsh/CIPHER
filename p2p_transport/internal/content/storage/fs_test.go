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

func TestFSStorage_ReadChunk_OversizedFileErrorBeforeAllocation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-chunk-file-id-12345678"))

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
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// Truncate file so size exceeds MaxChunkSize (1MB)
	oversizedLen := MaxChunkSize + 1024
	if err := f.Truncate(oversizedLen); err != nil {
		f.Close()
		t.Fatalf("failed to truncate file: %v", err)
	}
	f.Close()

	// Direct ReadChunk call
	_, err = ReadChunk(path)
	if err == nil {
		t.Fatal("expected error reading oversized chunk file, got nil")
	}

	// Call via GetChunk
	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected GetChunk error for oversized chunk file, got nil")
	}
}

func TestFSStorage_ReadChunk_ValidChunk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("valid-chunk-file-id-12345678901"))

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			PlainSize:  100,
			CipherSize: 128,
		},
		Data: make([]byte, 128),
	}
	for i := range chunk.Data {
		chunk.Data[i] = byte(i)
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("failed to put chunk: %v", err)
	}

	gotChunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("failed to get chunk: %v", err)
	}

	if len(gotChunk.Data) != 128 {
		t.Fatalf("expected data length 128, got %d", len(gotChunk.Data))
	}
}

func TestFSStorage_ReadChunk_NonExistentFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("non-existent-chunk-id-123456789"))

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for non-existent chunk file, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}
