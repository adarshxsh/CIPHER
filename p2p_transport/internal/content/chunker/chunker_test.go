package chunker

import (
	"bytes"
	"io"
	"testing"

	"cipher/internal/content/core"
)

func TestChunker_Split_ChannelCapacity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: 8,
	}
	c := NewChunker(config)

	// Create 10KB of data (10 chunks)
	data := make([]byte, 10*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunkCh, errCh := c.Split(reader)

	// Verify channel capacity
	if cap(chunkCh) != 8 {
		t.Errorf("expected channel capacity 8, got %d", cap(chunkCh))
	}

	var count int
	for chunk := range chunkCh {
		if chunk.Header.Index != uint32(count) {
			t.Errorf("expected chunk index %d, got %d", count, chunk.Header.Index)
		}
		c.PutBuffer(chunk.Data)
		count++
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected split error: %v", err)
	}

	if count != 10 {
		t.Errorf("expected 10 chunks, got %d", count)
	}
}

func TestChunker_Split_DefaultCapacity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024, // ChannelCapacity omitted (0)
	}
	c := NewChunker(config)

	reader := bytes.NewReader(make([]byte, 2048))
	chunkCh, errCh := c.Split(reader)

	if cap(chunkCh) != DefaultChannelCapacity {
		t.Errorf("expected default channel capacity %d, got %d", DefaultChannelCapacity, cap(chunkCh))
	}

	for chunk := range chunkCh {
		c.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_BufferRecycling(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       4096,
		ChannelCapacity: 4,
	}
	c := NewChunker(config)

	// Get a buffer, note its address / slice pointer, put it back
	buf1 := c.GetBuffer()
	if cap(buf1) < 4096+16 {
		t.Errorf("expected capacity >= %d, got %d", 4096+16, cap(buf1))
	}

	ptr1 := &buf1[0]
	c.PutBuffer(buf1)

	// Get another buffer from pool - should reuse buf1
	buf2 := c.GetBuffer()
	ptr2 := &buf2[0]

	if ptr1 != ptr2 {
		t.Logf("Note: Pool did not return identical pointer (may happen depending on runtime state), ptr1: %p, ptr2: %p", ptr1, ptr2)
	}
	c.PutBuffer(buf2)
}

func TestChunker_EOFHandling(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := NewChunker(config)

	reader := bytes.NewReader([]byte{})
	chunkCh, errCh := c.Split(reader)

	count := 0
	for range chunkCh {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 chunks for empty reader, got %d", count)
	}

	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("unexpected error on empty stream: %v", err)
	}
}
