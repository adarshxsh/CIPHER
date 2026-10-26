package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	id[0] = 0x12
	id[31] = 0x34

	payload := []byte("hello world payload data")
	chunkVal := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         id,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload)),
			CipherSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chunkVal); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	got, err := store.GetChunk(ctx, id)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if got.Header.ID != id {
		t.Errorf("expected ID %x, got %x", id, got.Header.ID)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("expected data %s, got %s", payload, got.Data)
	}
	if cap(got.Data) != len(payload) {
		t.Errorf("expected slice capacity %d, got %d", len(payload), cap(got.Data))
	}
}

func TestFSStorage_GetChunk_FileTooSmall(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	id[0] = 0xAA

	filePath := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write only 10 bytes (less than 66-byte header)
	if err := os.WriteFile(filePath, []byte("short file"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err := store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for file smaller than header size, got nil")
	}
}

func TestFSStorage_GetChunk_PayloadExceedsProtocolLimit(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	id[0] = 0xBB

	filePath := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: MaxCiphertextSize + 1,
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// Write oversized payload on disk
	oversizedData := make([]byte, MaxCiphertextSize+1)
	if _, err := f.Write(oversizedData); err != nil {
		f.Close()
		t.Fatalf("failed to write payload: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for payload exceeding protocol limit, got nil")
	}
}

func TestFSStorage_GetChunk_HeaderCipherSizeExceedsProtocolLimit(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	id[0] = 0xBC

	filePath := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: MaxCiphertextSize + 100,
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	// Payload on disk matches header.CipherSize so payloadDiskSize isn't caught first,
	// but CipherSize exceeds MaxCiphertextSize
	payload := make([]byte, MaxCiphertextSize+100)
	f.Write(payload)
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for CipherSize exceeding protocol limit, got nil")
	}
}

func TestFSStorage_GetChunk_PayloadSizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var id core.ChunkID
	id[0] = 0xCC

	filePath := store.pathForChunk(id)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	header := core.ChunkHeader{
		Version:    1,
		ID:         id,
		CipherSize: 20, // Header says 20
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// Disk file only has 10 payload bytes
	f.Write(make([]byte, 10))
	f.Close()

	_, err = store.GetChunk(ctx, id)
	if err == nil {
		t.Fatalf("expected error for payload size mismatch, got nil")
	}
}
