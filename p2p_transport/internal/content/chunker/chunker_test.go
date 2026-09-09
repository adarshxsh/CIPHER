package chunker_test

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_Split(t *testing.T) {
	chunkSize := uint32(1024) // 1KB
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := chunker.NewChunker(config)

	// Create 2.5KB of data (3 chunks: 1024, 1024, 512 bytes)
	dataSize := 2500
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	reader := bytes.NewReader(data)
	chunkCh, errCh := c.Split(reader)

	var chunks []*core.Chunk
	for chunk := range chunkCh {
		chunks = append(chunks, chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("chunker split returned error: %v", err)
	}

	expectedChunks := 3
	if len(chunks) != expectedChunks {
		t.Fatalf("expected %d chunks, got %d", expectedChunks, len(chunks))
	}

	// Verify headers and content
	var reassembled []byte
	for i, ch := range chunks {
		if ch.Header.Index != uint32(i) {
			t.Errorf("chunk %d has index %d", i, ch.Header.Index)
		}
		if i == 0 && ch.Header.PlainSize != chunkSize {
			t.Errorf("chunk 0 size %d != expected %d", ch.Header.PlainSize, chunkSize)
		}
		if i == 2 && ch.Header.PlainSize != 452 { // 2500 - 2048 = 452
			t.Errorf("chunk 2 size %d != expected 452", ch.Header.PlainSize)
		}
		reassembled = append(reassembled, ch.Data...)
	}

	if !bytes.Equal(data, reassembled) {
		t.Errorf("reassembled data does not match original input")
	}
}

func TestChunker_BufferReuseAndReslicing(t *testing.T) {
	chunkSize := uint32(4096)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := chunker.NewChunker(config)

	// Get a buffer, slice it shorter, and return to pool
	buf := c.GetBuffer()
	if len(buf) != int(chunkSize) {
		t.Fatalf("GetBuffer returned len %d != %d", len(buf), chunkSize)
	}

	shortBuf := buf[:100]
	c.PutBuffer(shortBuf)

	// Retrieve buffer from pool and ensure it was resliced back to full chunkSize
	reusedBuf := c.GetBuffer()
	if len(reusedBuf) != int(chunkSize) {
		t.Errorf("Reused buffer len %d != expected %d after PutBuffer", len(reusedBuf), chunkSize)
	}
	if cap(reusedBuf) < int(chunkSize) {
		t.Errorf("Reused buffer cap %d < expected %d", cap(reusedBuf), chunkSize)
	}
}

func TestChunker_CustomCapacity(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	customCap := 32
	c := chunker.NewChunkerWithCapacity(config, customCap)

	data := make([]byte, 10*1024)
	reader := bytes.NewReader(data)
	chunkCh, _ := c.Split(reader)

	if cap(chunkCh) != customCap {
		t.Errorf("channel capacity %d != expected %d", cap(chunkCh), customCap)
	}

	for range chunkCh {
	}
}

func TestChunker_Concurrent(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 2048}
	c := chunker.NewChunker(config)

	var wg sync.WaitGroup
	workers := 10
	iterations := 20

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 100*1024)
			rand.Read(data)

			for i := 0; i < iterations; i++ {
				reader := bytes.NewReader(data)
				chunkCh, errCh := c.Split(reader)

				for ch := range chunkCh {
					buf := ch.Data
					c.PutBuffer(buf)
				}

				if err := <-errCh; err != nil {
					t.Errorf("concurrent split error: %v", err)
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkChunker_Split(b *testing.B) {
	chunkSize := uint32(256 * 1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := chunker.NewChunker(config)

	dataSize := 10 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)

	b.SetBytes(int64(dataSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		chunkCh, errCh := c.Split(reader)

		for ch := range chunkCh {
			c.PutBuffer(ch.Data)
		}

		if err := <-errCh; err != nil {
			b.Fatalf("Split failed: %v", err)
		}
	}
}
