package chunker_test

import (
	"bytes"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_Split_ChannelCapacity(t *testing.T) {
	// 1. Default buffer capacity
	configDefault := core.EngineConfig{ChunkSize: 1024}
	cDefault := chunker.NewChunker(configDefault)
	data := make([]byte, 1024*5)
	chunkCh, errCh := cDefault.Split(bytes.NewReader(data))
	if cap(chunkCh) != core.DefaultChannelBufferCap {
		t.Errorf("expected default channel capacity %d, got %d", core.DefaultChannelBufferCap, cap(chunkCh))
	}
	// Drain
	for range chunkCh {
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2. Custom buffer capacity
	customCap := 128
	configCustom := core.EngineConfig{
		ChunkSize:        1024,
		ChannelBufferCap: customCap,
	}
	cCustom := chunker.NewChunker(configCustom)
	chunkChCustom, errChCustom := cCustom.Split(bytes.NewReader(data))
	if cap(chunkChCustom) != customCap {
		t.Errorf("expected custom channel capacity %d, got %d", customCap, cap(chunkChCustom))
	}
	for range chunkChCustom {
	}
	if err := <-errChCustom; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_SyncPool_BufferReuse(t *testing.T) {
	chunkSize := uint32(1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := chunker.NewChunker(config)

	// Get a buffer from the chunker pool
	buf1 := c.GetBuffer(chunkSize)
	if cap(buf1) != int(chunkSize) {
		t.Fatalf("expected buffer capacity %d, got %d", chunkSize, cap(buf1))
	}

	// Fill buf1 with dummy data
	for i := range buf1 {
		buf1[i] = 0xAA
	}

	// Return buffer to pool
	c.PutBuffer(buf1)

	// Get buffer again from pool; should reuse the same backing array memory
	buf2 := c.GetBuffer(chunkSize)

	// Verify buffer contents were cleared upon return to pool
	for i, b := range buf2 {
		if b != 0 {
			t.Errorf("expected cleared byte at index %d, got 0x%X", i, b)
		}
	}

	c.PutBuffer(buf2)
}

func TestChunker_NonStandardChunkSizeFallback(t *testing.T) {
	standardSize := uint32(1024)
	config := core.EngineConfig{ChunkSize: standardSize}
	c := chunker.NewChunker(config)

	// Get non-standard size buffer
	nonStandardSize := uint32(500)
	bufNonStandard := c.GetBuffer(nonStandardSize)
	if len(bufNonStandard) != int(nonStandardSize) {
		t.Fatalf("expected length %d, got %d", nonStandardSize, len(bufNonStandard))
	}

	// Return non-standard buffer to pool; should be ignored cleanly
	c.PutBuffer(bufNonStandard)

	// Getting a standard size buffer should get a standard sized buffer
	bufStandard := c.GetBuffer(standardSize)
	if cap(bufStandard) != int(standardSize) {
		t.Fatalf("expected capacity %d, got %d", standardSize, cap(bufStandard))
	}
	c.PutBuffer(bufStandard)
}

func TestChunker_Split_EOF_And_ShortReads(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	c := chunker.NewChunker(config)

	// Empty reader test
	emptyReader := bytes.NewReader([]byte{})
	chunkCh, errCh := c.Split(emptyReader)

	count := 0
	for range chunkCh {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 chunks for empty reader, got %d", count)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("expected nil error for EOF, got %v", err)
	}

	// Short read test (1500 bytes with 1024 chunk size = 2 chunks)
	data := make([]byte, 1500)
	chunkCh2, errCh2 := c.Split(bytes.NewReader(data))

	chunks := []*core.Chunk{}
	for chunk := range chunkCh2 {
		chunks = append(chunks, chunk)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0].Header.PlainSize != 1024 {
		t.Errorf("expected first chunk size 1024, got %d", chunks[0].Header.PlainSize)
	}
	if chunks[1].Header.PlainSize != 476 {
		t.Errorf("expected second chunk size 476, got %d", chunks[1].Header.PlainSize)
	}

	if err := <-errCh2; err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
