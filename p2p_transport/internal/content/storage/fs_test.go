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

	var chunkID core.ChunkID
	chunkID[0] = 0x12
	chunkID[1] = 0x34

	payload := []byte("hello world ciphertext")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload) - 5),
			CipherSize: uint32(len(payload)),
			Nonce:      [12]byte{1, 2, 3},
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Fatalf("HasChunk failed or returned false: err=%v, has=%v", err, has)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if retrieved.Header.ID != chunk.Header.ID {
		t.Errorf("chunk ID mismatch")
	}
	if retrieved.Header.CipherSize != chunk.Header.CipherSize {
		t.Errorf("CipherSize mismatch")
	}
	if string(retrieved.Data) != string(payload) {
		t.Errorf("data mismatch: expected %s, got %s", payload, retrieved.Data)
	}
}

func TestFSStorage_GetChunk_TruncatedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-truncated-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xAA

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write only 20 bytes (less than ChunkHeaderSize=66)
	truncatedData := make([]byte, 20)
	if err := os.WriteFile(filePath, truncatedData, 0644); err != nil {
		t.Fatalf("failed to write truncated chunk file: %v", err)
	}

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for truncated chunk file, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedFrame(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-oversized-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xBB

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Create file size larger than MaxFrameSize (2 MiB)
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if err := f.Truncate(MaxFrameSize + 10); err != nil {
		f.Close()
		t.Fatalf("failed to truncate file: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized chunk file exceeding MaxFrameSize, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedPayload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-payload-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xCC

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write header with payload size exceeding MaxFrameSize - ChunkHeaderSize
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	oversizedPayloadLen := uint32(MaxFrameSize + 100)
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: oversizedPayloadLen,
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for chunk payload exceeding protocol bound, got nil")
	}
}

func TestFSStorage_GetChunk_HeaderPayloadMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-mismatch-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	chunkID[0] = 0xDD

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Header says CipherSize = 500
	header := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 500,
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// But write only 100 bytes of data
	if _, err := f.Write(make([]byte, 100)); err != nil {
		f.Close()
		t.Fatalf("failed to write data: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for header payload size mismatch, got nil")
	}
}

func TestFSStorage_GetChunk_UnencryptedAndEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-plain-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	// 1. Unencrypted chunk (CipherSize = 0, PlainSize = 12)
	var chunkID1 core.ChunkID
	chunkID1[0] = 0xE1
	plainTextData := []byte("plain text!!")
	chunk1 := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			ID:        chunkID1,
			PlainSize: uint32(len(plainTextData)),
		},
		Data: plainTextData,
	}
	if err := store.PutChunk(ctx, chunk1); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	got1, err := store.GetChunk(ctx, chunkID1)
	if err != nil {
		t.Fatalf("GetChunk failed for unencrypted chunk: %v", err)
	}
	if string(got1.Data) != string(plainTextData) {
		t.Errorf("unencrypted chunk data mismatch")
	}

	// 2. Empty chunk (0 byte payload)
	var chunkID2 core.ChunkID
	chunkID2[0] = 0xE2
	chunk2 := &core.Chunk{
		Header: core.ChunkHeader{
			Version: 1,
			ID:      chunkID2,
		},
		Data: []byte{},
	}
	if err := store.PutChunk(ctx, chunk2); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	got2, err := store.GetChunk(ctx, chunkID2)
	if err != nil {
		t.Fatalf("GetChunk failed for empty chunk: %v", err)
	}
	if len(got2.Data) != 0 {
		t.Errorf("expected 0 byte payload, got %d", len(got2.Data))
	}
}
