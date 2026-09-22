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

func TestFSStorage_PutAndGetChunk_Encrypted(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("01234567890123456789012345678901"))

	payload := []byte("encrypted_payload_data_32bytes!!")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			Index:      0,
			Offset:     0,
			PlainSize:  32,
			CipherSize: uint32(len(payload)),
			Nonce:      [12]byte{1, 2, 3},
		},
		Data: payload,
	}

	err := store.PutChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	has, err := store.HasChunk(ctx, id)
	if err != nil || !has {
		t.Fatalf("HasChunk expected true, got has=%v err=%v", has, err)
	}

	retrieved, err := store.GetChunk(ctx, id)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if retrieved.Header.ID != chunk.Header.ID {
		t.Errorf("expected ChunkID %v, got %v", chunk.Header.ID, retrieved.Header.ID)
	}

	if !bytes.Equal(retrieved.Data, payload) {
		t.Errorf("expected data %q, got %q", payload, retrieved.Data)
	}
}

func TestFSStorage_PutAndGetChunk_Plaintext(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("plaintext01234567890123456789012"))

	payload := []byte("plaintext_payload_data_32_bytes!")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			Index:      1,
			Offset:     32,
			PlainSize:  uint32(len(payload)),
			CipherSize: 0,
			Nonce:      [12]byte{},
		},
		Data: payload,
	}

	err := store.PutChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	retrieved, err := store.GetChunk(ctx, id)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if !bytes.Equal(retrieved.Data, payload) {
		t.Errorf("expected data %q, got %q", payload, retrieved.Data)
	}
}

func TestFSStorage_GetChunk_TruncatedHeader(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("truncatedheader01234567890123456"))

	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Write only 10 bytes (header requires 66 bytes)
	if err := os.WriteFile(path, []byte("too_short!"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for truncated header, got nil")
	}
}

func TestFSStorage_GetChunk_TruncatedPayload(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("truncatedpayload0123456789012345"))

	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: 100, // Claims 100 payload bytes
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		t.Fatalf("Write header failed: %v", err)
	}
	// Write only 10 payload bytes instead of 100
	if _, err := f.Write([]byte("1234567890")); err != nil {
		t.Fatalf("Write payload failed: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for truncated payload, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedPayload(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("oversizedpayload0123456789012345"))

	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: MaxChunkPayloadSize + 1, // Exceeds limit
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		t.Fatalf("Write header failed: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for oversized payload header, got nil")
	}
}

func TestFSStorage_GetChunk_TrailingBytes(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("trailingbytes0123456789012345678"))

	path := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	payload := []byte("valid_payload_bytes_12345")
	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: uint32(len(payload)),
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		t.Fatalf("Write header failed: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatalf("Write payload failed: %v", err)
	}
	// Append extra trailing junk bytes
	if _, err := f.Write([]byte("extra_trailing_junk_bytes")); err != nil {
		t.Fatalf("Write trailing failed: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for chunk file with trailing bytes, got nil")
	}
}

func TestFSStorage_GetChunk_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	copy(id[:], []byte("nonexistentfile01234567890123456"))

	_, err := store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for non-existent file, got nil")
	}
}
