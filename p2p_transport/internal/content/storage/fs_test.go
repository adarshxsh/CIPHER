package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_BufferPooling(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	payload := []byte("hello pooled buffer chunk storage reader")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			ID:         chunkID,
			Index:      0,
			CipherSize: uint32(len(payload)),
			PlainSize:  uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("failed to put chunk: %v", err)
	}

	retrieved, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("failed to get chunk: %v", err)
	}

	if !bytes.Equal(retrieved.Data, payload) {
		t.Fatalf("expected payload %q, got %q", payload, retrieved.Data)
	}

	if cap(retrieved.Data) < MaxChunkSize {
		t.Fatalf("expected buffer capacity >= %d, got %d", MaxChunkSize, cap(retrieved.Data))
	}

	// Return buffer to pool
	PutBuffer(retrieved.Data)

	// Fetch buffer from pool and ensure capacity is maintained
	buf := GetBuffer()
	if cap(buf) < MaxChunkSize {
		t.Fatalf("expected pooled buffer capacity >= %d, got %d", MaxChunkSize, cap(buf))
	}
	PutBuffer(buf)
}

func TestFSStorage_GetChunk_SizeGuardrails(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Case 1: File too small for header
	if err := os.WriteFile(path, []byte("short"), 0644); err != nil {
		t.Fatalf("failed to write short file: %v", err)
	}

	_, err := store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for file smaller than header")
	}

	// Case 2: Extra trailing data beyond exact chunk size bounds
	payload := []byte("valid payload")
	c := &core.Chunk{
		Header: core.ChunkHeader{
			ID:         chunkID,
			CipherSize: uint32(len(payload)),
			PlainSize:  uint32(len(payload)),
		},
		Data: payload,
	}
	if err := store.PutChunk(ctx, c); err != nil {
		t.Fatalf("failed to put chunk: %v", err)
	}

	// Append trailing extra byte
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open chunk for append: %v", err)
	}
	f.Write([]byte{0xFF})
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for chunk file containing extra trailing bytes")
	}
}

func BenchmarkFSStorage_GetChunk(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "fs-storage-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store := NewFSStore(tmpDir)
	ctx := context.Background()

	chunkSize := 256 * 1024 // 256 KiB chunk
	payload := make([]byte, chunkSize)
	rand.Read(payload)

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			ID:         chunkID,
			CipherSize: uint32(chunkSize),
			PlainSize:  uint32(chunkSize),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		b.Fatalf("failed to put chunk: %v", err)
	}

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		retrieved, err := store.GetChunk(ctx, chunkID)
		if err != nil {
			b.Fatalf("GetChunk failed: %v", err)
		}
		PutBuffer(retrieved.Data)
	}
}
