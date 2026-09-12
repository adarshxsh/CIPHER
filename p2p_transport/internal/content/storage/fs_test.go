package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_PreAllocationAndSizeChecks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if err := NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	// 1. Normal Put and Get Chunk
	var chunkID core.ChunkID
	copy(chunkID[:], []byte("01234567890123456789012345678901"))

	payload := []byte("hello world payload data")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payload)),
			CipherSize: uint32(len(payload)),
			Nonce:      [12]byte{1, 2, 3},
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if !bytes.Equal(retrieved.Data, payload) {
		t.Fatalf("retrieved data mismatch: got %s, want %s", string(retrieved.Data), string(payload))
	}

	if retrieved.Header.Version != chunk.Header.Version || retrieved.Header.ID != chunk.Header.ID {
		t.Fatalf("retrieved header mismatch")
	}

	// 2. Small/Corrupted Chunk File (< headerSize)
	var corruptID core.ChunkID
	copy(corruptID[:], []byte("corrupt1234567890123456789012345"))

	corruptPath := store.pathForChunk(corruptID)
	if err := os.MkdirAll(filepath.Dir(corruptPath), 0755); err != nil {
		t.Fatalf("failed to create corrupt dir: %v", err)
	}
	if err := os.WriteFile(corruptPath, []byte("too short"), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	_, err = store.GetChunk(ctx, corruptID)
	if err == nil {
		t.Fatalf("expected error reading corrupt chunk file smaller than header size, got nil")
	}

	// 3. Oversized Chunk Payload (> MaxChunkPayloadSize)
	var oversizedID core.ChunkID
	copy(oversizedID[:], []byte("oversized23456789012345678901234"))

	oversizedPath := store.pathForChunk(oversizedID)
	if err := os.MkdirAll(filepath.Dir(oversizedPath), 0755); err != nil {
		t.Fatalf("failed to create oversized dir: %v", err)
	}

	f, err := os.Create(oversizedPath)
	if err != nil {
		t.Fatalf("failed to create oversized file: %v", err)
	}

	// Write dummy 66 byte header
	headerBytes := make([]byte, 66)
	f.Write(headerBytes)

	// Truncate file to exceed MaxChunkPayloadSize + headerSize
	targetSize := MaxChunkPayloadSize + 66 + 100
	if err := f.Truncate(targetSize); err != nil {
		f.Close()
		t.Fatalf("failed to truncate oversized file: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, oversizedID)
	if err == nil {
		t.Fatalf("expected error reading oversized chunk payload, got nil")
	}
}

func TestFSStorage_ManifestPreAllocation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-manifest-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if err := NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var contentID core.ContentID
	copy(contentID[:], []byte("manifest123456789012345678901234"))

	manifestData := []byte(`{"version":1,"descriptor":{"id":"123","type":"file","size":100}}`)
	if err := store.PutManifestBytes(ctx, contentID, manifestData); err != nil {
		t.Fatalf("PutManifestBytes failed: %v", err)
	}

	retrieved, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("GetManifestBytes failed: %v", err)
	}

	if !bytes.Equal(retrieved, manifestData) {
		t.Fatalf("manifest data mismatch: got %s, want %s", string(retrieved), string(manifestData))
	}
}
