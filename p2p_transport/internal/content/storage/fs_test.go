package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_EncryptedSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("12345678901234567890123456789012"))

	payload := []byte("encrypted_payload_data_here_123456")
	c := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload) - 16),
			CipherSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, c); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	got, err := store.GetChunk(ctx, id)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if got.Header.CipherSize != c.Header.CipherSize {
		t.Errorf("expected CipherSize %d, got %d", c.Header.CipherSize, got.Header.CipherSize)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("expected data %q, got %q", payload, got.Data)
	}
}

func TestFSStorage_GetChunk_UnencryptedSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("unencrypted_chunk_id_12345678901"))

	payload := []byte("unencrypted_plain_payload_content")
	c := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			Index:      1,
			Offset:     100,
			PlainSize:  uint32(len(payload)),
			CipherSize: 0,
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, c); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	got, err := store.GetChunk(ctx, id)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if got.Header.PlainSize != c.Header.PlainSize {
		t.Errorf("expected PlainSize %d, got %d", c.Header.PlainSize, got.Header.PlainSize)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("expected data %q, got %q", payload, got.Data)
	}
}

func TestFSStorage_GetChunk_ZeroSizeHeader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("zero_size_chunk_id_1234567890123"))

	c := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			PlainSize:  0,
			CipherSize: 0,
		},
		Data: []byte{},
	}

	if err := store.PutChunk(ctx, c); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatal("expected GetChunk to fail for zero payload size header, but it succeeded")
	}
}

func TestFSStorage_GetChunk_OversizedHeader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("oversized_chunk_id_1234567890123"))

	c := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			CipherSize: MaxCiphertextSize + 100,
		},
		Data: make([]byte, 10),
	}

	if err := store.PutChunk(ctx, c); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatal("expected GetChunk to fail for oversized payload header, but it succeeded")
	}
}

func TestFSStorage_GetChunk_TruncatedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("truncated_chunk_id_1234567890123"))

	// Put Chunk with CipherSize = 100, but write only 20 bytes
	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: 100,
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("binary.Write failed: %v", err)
	}
	// Write less than expected
	_, _ = f.Write([]byte("short payload"))
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatal("expected GetChunk to fail for truncated chunk file, but it succeeded")
	}
}

func TestFSStorage_GetChunk_TrailingBytes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs_storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("trailing_chunk_id_12345678901234"))

	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	payload := []byte("exact_payload_bytes")
	hdr := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: uint32(len(payload)),
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatalf("binary.Write failed: %v", err)
	}
	_, _ = f.Write(payload)
	// Write extra trailing bytes
	_, _ = f.Write([]byte("extra_garbage_bytes"))
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatal("expected GetChunk to fail due to trailing bytes, but it succeeded")
	}
}
