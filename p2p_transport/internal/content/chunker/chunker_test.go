package chunker_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_ChannelCapacity(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	c := chunker.NewChunker(config)

	r := bytes.NewReader(make([]byte, 100))
	chunkCh, _ := c.Split(r)

	if cap(chunkCh) != chunker.DefaultChannelCapacity {
		t.Fatalf("expected channel capacity %d, got %d", chunker.DefaultChannelCapacity, cap(chunkCh))
	}
}

func TestChunker_GetAndPutBuffer(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	c := chunker.NewChunker(config)

	buf := c.GetBuffer()
	if len(buf) != int(config.ChunkSize) {
		t.Fatalf("expected buffer length %d, got %d", config.ChunkSize, len(buf))
	}
	if cap(buf) < int(config.ChunkSize)+64 {
		t.Fatalf("expected buffer capacity >= %d, got %d", config.ChunkSize+64, cap(buf))
	}

	// Fill buffer with data
	for i := range buf {
		buf[i] = 0xAA
	}

	c.PutBuffer(buf)

	// Get buffer from pool again and ensure it was cleared
	reusedBuf := c.GetBuffer()
	for i, b := range reusedBuf {
		if b != 0 {
			t.Fatalf("expected buffer byte at index %d to be zeroed, got 0x%X", i, b)
		}
	}
}

func TestChunker_Split(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	c := chunker.NewChunker(config)

	dataSize := 100 * 1024 // 100 KB => 4 chunks (32KB, 32KB, 32KB, 4KB)
	originalData := make([]byte, dataSize)
	rand.Read(originalData)

	r := bytes.NewReader(originalData)
	chunkCh, errCh := c.Split(r)

	var reassembled bytes.Buffer
	chunkCount := 0

	for chunk := range chunkCh {
		chunkCount++
		reassembled.Write(chunk.Data)
		c.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error during Split: %v", err)
	}

	if chunkCount != 4 {
		t.Fatalf("expected 4 chunks, got %d", chunkCount)
	}

	if !bytes.Equal(reassembled.Bytes(), originalData) {
		t.Fatal("reassembled data does not match original data")
	}
}

func BenchmarkAllocations(b *testing.B) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	c := chunker.NewChunker(config)

	dataSize := 10 * 1024 * 1024 // 10 MB
	data := make([]byte, dataSize)
	rand.Read(data)

	b.SetBytes(int64(dataSize))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		chunkCh, errCh := c.Split(r)

		for chunk := range chunkCh {
			c.PutBuffer(chunk.Data)
		}

		if err := <-errCh; err != nil {
			b.Fatalf("Split failed: %v", err)
		}
	}
}
