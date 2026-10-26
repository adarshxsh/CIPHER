package chunker_test

import (
	"bytes"
	"io"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_BufferedChannel(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:         1024,
		ChannelBufferSize: 32,
	}

	c := chunker.NewChunker(config)

	// Create data for 10 chunks
	data := make([]byte, 1024*10)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	chunkCh, errCh := c.Split(reader)

	// Verify channel is buffered
	if cap(chunkCh) != 32 {
		t.Fatalf("expected channel buffer capacity 32, got %d", cap(chunkCh))
	}

	var collected []byte
	chunkCount := 0

	for chunk := range chunkCh {
		chunkCount++
		collected = append(collected, chunk.Data...)
	}

	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("unexpected chunker error: %v", err)
	}

	if chunkCount != 10 {
		t.Fatalf("expected 10 chunks, got %d", chunkCount)
	}

	if !bytes.Equal(collected, data) {
		t.Fatalf("chunker output data does not match input")
	}
}

func TestChunker_DefaultChannelBufferSize(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
		// ChannelBufferSize unspecified (0)
	}

	c := chunker.NewChunker(config)
	reader := bytes.NewReader(make([]byte, 2048))
	chunkCh, _ := c.Split(reader)

	if cap(chunkCh) <= 0 {
		t.Fatalf("expected buffered channel with default capacity > 0, got %d", cap(chunkCh))
	}
	
	// Drain channel
	for range chunkCh {
	}
}
