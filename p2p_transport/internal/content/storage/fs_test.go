package storage

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_Success(t *testing.T) {
	tmpDir := t.TempDir()
	if err := NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	payload := []byte("hello world bounded reader test")
	originalChunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:   1,
			ID:        chunkID,
			Index:     0,
			Offset:    0,
			PlainSize: uint32(len(payload)),
		},
		Data: payload,
	}

	if err := store.PutChunk(ctx, originalChunk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Fatalf("HasChunk failed: has=%v, err=%v", has, err)
	}

	readChunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if string(readChunk.Data) != string(payload) {
		t.Errorf("GetChunk data mismatch: got %q, want %q", string(readChunk.Data), string(payload))
	}
	if readChunk.Header.ID != chunkID {
		t.Errorf("GetChunk header ID mismatch")
	}
}

func TestFSStorage_GetChunk_TruncatedHeader(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	// Write less than MinHeaderSize (66 bytes)
	if err := os.WriteFile(filePath, []byte("too short"), 0644); err != nil {
		t.Fatalf("failed to write truncated chunk: %v", err)
	}

	_, err := store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for truncated chunk file, got nil")
	}
}

func TestFSStorage_GetChunk_OversizedChunk(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	var chunkID core.ChunkID
	rand.Read(chunkID[:])

	filePath := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create chunk file: %v", err)
	}

	header := core.ChunkHeader{
		Version:   1,
		ID:        chunkID,
		PlainSize: uint32(MaxChunkSize + 100),
	}
	if err := binary.Write(f, binary.LittleEndian, &header); err != nil {
		f.Close()
		t.Fatalf("failed to write header: %v", err)
	}

	// Truncate/extend file size to exceed MaxChunkSize
	if err := f.Truncate(int64(MinHeaderSize + MaxChunkSize + 100)); err != nil {
		f.Close()
		t.Fatalf("failed to truncate: %v", err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatal("expected error for oversized chunk exceeding MaxChunkSize, got nil")
	}
}

func TestFSStorage_GetChunk_ConcurrentPoolReuse(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewFSStore(tmpDir)
	ctx := context.Background()

	const numWorkers = 20
	const chunksPerWorker = 10

	var chunkIDs []core.ChunkID

	for i := 0; i < chunksPerWorker; i++ {
		var id core.ChunkID
		rand.Read(id[:])
		chunkIDs = append(chunkIDs, id)

		data := make([]byte, 1024)
		rand.Read(data)

		chunk := &core.Chunk{
			Header: core.ChunkHeader{
				Version:   1,
				ID:        id,
				PlainSize: uint32(len(data)),
			},
			Data: data,
		}
		if err := store.PutChunk(ctx, chunk); err != nil {
			t.Fatalf("failed to put chunk: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			for _, id := range chunkIDs {
				ch, err := store.GetChunk(ctx, id)
				if err != nil {
					t.Errorf("concurrent GetChunk failed: %v", err)
					return
				}
				if ch == nil || len(ch.Data) != 1024 {
					t.Errorf("concurrent GetChunk returned invalid data")
					return
				}
			}
		}()
	}

	wg.Wait()
}
