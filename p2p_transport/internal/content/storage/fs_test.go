package storage

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_PutAndGetChunk(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	data := []byte("hello world storage test payload")
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			ID:        chunkID,
			Index:     0,
			Offset:    0,
			PlainSize: uint32(len(data)),
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
		t.Fatalf("got data %q, want %q", string(retrieved.Data), string(data))
	}
}

func TestFSStorage_GetChunk_TruncatedHeader(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write only 10 bytes (less than 66 bytes header size)
	if err := os.WriteFile(chunkPath, []byte("too short!"), 0644); err != nil {
		t.Fatalf("failed to write truncated file: %v", err)
	}

	_, err := store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for truncated header chunk, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedFile(t *testing.T) {
	tmpDir := t.TempDir()
	// Set max chunk size to 100 bytes for test
	store := NewFSStore(tmpDir, WithMaxChunkSize(100))
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Write a file of 200 bytes total (exceeds max 100 + 66)
	oversized := make([]byte, 200)
	if err := os.WriteFile(chunkPath, oversized, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err := store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for oversized chunk file, got nil")
	}
}

func TestFSStorage_GetChunk_PayloadExceedsMax(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir, WithMaxChunkSize(100))
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Write header that claims PlainSize = 500 (exceeds max 100)
	header := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: 500,
	}
	binary.Write(f, binary.LittleEndian, &header)
	// Write 500 bytes of data
	f.Write(make([]byte, 500))
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error when header claims payload > maxChunkSize, got nil")
	}
}

func TestFSStorage_GetChunk_PayloadSizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunkPath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	f, err := os.Create(chunkPath)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Write header claiming 100 bytes payload
	header := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: 100,
	}
	binary.Write(f, binary.LittleEndian, &header)
	// Write only 50 bytes of data on disk
	f.Write(make([]byte, 50))
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error when header payload size mismatches file size, got nil")
	}
}

func TestFSStorage_GetManifestBytes_ValidAndExceedsLimit(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir, WithMaxManifestSize(100))
	ctx := context.Background()

	var contentID core.ContentID
	rand.Read(contentID[:])

	// Put valid manifest <= 100 bytes
	manifestData := []byte(`{"version":1,"name":"test"}`)
	if err := store.PutManifestBytes(ctx, contentID, manifestData); err != nil {
		t.Fatalf("failed to put manifest: %v", err)
	}

	retrieved, err := store.GetManifestBytes(ctx, contentID)
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}
	if string(retrieved) != string(manifestData) {
		t.Fatalf("got manifest %q, want %q", string(retrieved), string(manifestData))
	}

	// Create oversized manifest exceeding limit (150 bytes)
	var contentID2 core.ContentID
	rand.Read(contentID2[:])

	oversizedManifest := make([]byte, 150)
	manifestPath := store.manifestPath(contentID2)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0755); err != nil {
		t.Fatalf("failed to create manifest dir: %v", err)
	}
	if err := os.WriteFile(manifestPath, oversizedManifest, 0644); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	_, err = store.GetManifestBytes(ctx, contentID2)
	if err == nil {
		t.Fatal("expected error for manifest exceeding size threshold, got nil")
	}
}

func BenchmarkFSStorage_GetChunk(b *testing.B) {
	tmpDir := b.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	data := make([]byte, 256*1024) // 256 KiB
	rand.Read(data)

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			ID:        chunkID,
			PlainSize: uint32(len(data)),
		},
		Data: data,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		b.Fatalf("failed to put chunk: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c, err := store.GetChunk(ctx, chunkID)
		if err != nil || len(c.Data) != len(data) {
			b.Fatalf("failed to get chunk: %v", err)
		}
	}
}

func BenchmarkFSStorage_GetChunk_Parallel(b *testing.B) {
	tmpDir := b.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	data := make([]byte, 64*1024) // 64 KiB
	rand.Read(data)

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			ID:        chunkID,
			PlainSize: uint32(len(data)),
		},
		Data: data,
	}

	if err := store.PutChunk(ctx, chunk); err != nil {
		b.Fatalf("failed to put chunk: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c, err := store.GetChunk(ctx, chunkID)
			if err != nil || len(c.Data) != len(data) {
				b.Fatalf("failed to get chunk in parallel: %v", err)
			}
		}
	})
}
