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
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	// 1. Test Encrypted Chunk
	var chunkID1 core.ChunkID
	copy(chunkID1[:], []byte("encrypted-chunk-id-123456789012"))
	encChunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID1,
			Index:      0,
			Offset:     0,
			PlainSize:  100,
			CipherSize: 128,
		},
		Data: make([]byte, 128),
	}
	for i := range encChunk.Data {
		encChunk.Data[i] = byte(i)
	}

	if err := store.PutChunk(ctx, encChunk); err != nil {
		t.Fatalf("failed to put encrypted chunk: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID1)
	if err != nil || !has {
		t.Fatalf("expected HasChunk to be true, got %v, err=%v", has, err)
	}

	gotChunk, err := store.GetChunk(ctx, chunkID1)
	if err != nil {
		t.Fatalf("failed to get encrypted chunk: %v", err)
	}

	if gotChunk.Header.CipherSize != encChunk.Header.CipherSize {
		t.Errorf("expected CipherSize %d, got %d", encChunk.Header.CipherSize, gotChunk.Header.CipherSize)
	}
	if len(gotChunk.Data) != 128 {
		t.Errorf("expected data length 128, got %d", len(gotChunk.Data))
	}

	// 2. Test Plaintext Chunk (CipherSize == 0)
	var chunkID2 core.ChunkID
	copy(chunkID2[:], []byte("plaintext-chunk-id-123456789012"))
	plainChunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID2,
			Index:      1,
			Offset:     100,
			PlainSize:  64,
			CipherSize: 0,
		},
		Data: make([]byte, 64),
	}
	for i := range plainChunk.Data {
		plainChunk.Data[i] = byte(i + 10)
	}

	if err := store.PutChunk(ctx, plainChunk); err != nil {
		t.Fatalf("failed to put plain chunk: %v", err)
	}

	gotPlain, err := store.GetChunk(ctx, chunkID2)
	if err != nil {
		t.Fatalf("failed to get plain chunk: %v", err)
	}
	if len(gotPlain.Data) != 64 {
		t.Errorf("expected data length 64, got %d", len(gotPlain.Data))
	}
}

func TestFSStorage_GetChunk_HeaderCipherSizeExceedsMax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-ciphersize-id-1234567"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		PlainSize:  100,
		CipherSize: uint32(core.MaxCiphertextSize + 100),
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Write matching dummy payload bytes
	f.Write(make([]byte, hdr.CipherSize))

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for CipherSize exceeding MaxCiphertextSize, got nil")
	}
}

func TestFSStorage_GetChunk_HeaderPlainSizeExceedsMax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-plainsize-id-12345670"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		PlainSize:  uint32(core.MaxCiphertextSize + 500),
		CipherSize: 0,
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for PlainSize exceeding MaxCiphertextSize, got nil")
	}
}

func TestFSStorage_GetChunk_FilePayloadExceedsMax(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("oversized-disk-file-id-12345678"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		PlainSize:  100,
		CipherSize: 100,
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Write file payload larger than MaxCiphertextSize
	f.Write(make([]byte, core.MaxCiphertextSize+1000))

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for disk payload exceeding MaxCiphertextSize, got nil")
	}
}

func TestFSStorage_GetChunk_PayloadSizeMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("mismatched-size-id-123456789012"))

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		PlainSize:  100,
		CipherSize: 100,
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Write only 50 bytes when header says 100
	f.Write(make([]byte, 50))

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for payload size mismatch, got nil")
	}
}
