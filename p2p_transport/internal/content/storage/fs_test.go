package storage

import (
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

	payload := []byte("hello world ciphertext payload")

	chk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len("hello world")),
			CipherSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if retrieved.Header.ID != chunkID {
		t.Errorf("chunk ID mismatch: got %v, want %v", retrieved.Header.ID, chunkID)
	}
	if string(retrieved.Data) != string(payload) {
		t.Errorf("chunk data mismatch: got %s, want %s", string(retrieved.Data), string(payload))
	}
}

func TestFSStorage_GetChunk_OversizedCipherSize(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-oversized-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("11111111111111111111111111111111"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dirs: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: MaxCiphertextSize + 100, // Oversized
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized CipherSize, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedPlainSize(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-oversized-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("22222222222222222222222222222222"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dirs: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: MaxCiphertextSize + 500, // Oversized
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized PlainSize, got nil")
	}
}

func TestFSStorage_GetChunk_TrailingBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-trailing-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("33333333333333333333333333333333"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dirs: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	payload := []byte("valid payload")
	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: uint32(len(payload)),
	}

	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		t.Fatalf("failed to write payload: %v", err)
	}
	// Append extra trailing bytes
	if _, err := f.Write([]byte("extra trailing data")); err != nil {
		f.Close()
		t.Fatalf("failed to write trailing bytes: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for file with trailing bytes, got nil")
	}
}

func TestFSStorage_GetChunk_TruncatedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fsstorage-truncated-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("44444444444444444444444444444444"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dirs: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 100, // Header expects 100 bytes
	}

	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	// Write only 10 bytes instead of 100
	if _, err := f.Write([]byte("short data")); err != nil {
		f.Close()
		t.Fatalf("failed to write short payload: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for truncated file, got nil")
	}
}
