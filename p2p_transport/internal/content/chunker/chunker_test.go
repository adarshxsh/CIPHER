package chunker

import (
	"bytes"
	"io"
	"testing"

	"cipher/internal/content/core"
)

func TestChunker_DefaultChannelCapacity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := NewChunker(config)

	r := bytes.NewReader([]byte("test data"))
	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != DefaultChannelCapacity {
		t.Fatalf("expected channel capacity %d, got %d", DefaultChannelCapacity, cap(chunkCh))
	}

	for chunk := range chunkCh {
		c.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_CustomChannelCapacity(t *testing.T) {
	customCap := 32
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: customCap,
	}
	c := NewChunker(config)

	r := bytes.NewReader([]byte("test data"))
	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != customCap {
		t.Fatalf("expected channel capacity %d, got %d", customCap, cap(chunkCh))
	}

	for chunk := range chunkCh {
		c.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_BufferPoolAndMetadata(t *testing.T) {
	chunkSize := uint32(100)
	config := core.EngineConfig{
		ChunkSize: chunkSize,
	}
	c := NewChunker(config)

	// Create 250 bytes of data (should result in 3 chunks: 100, 100, 50)
	data := make([]byte, 250)
	for i := range data {
		data[i] = byte(i % 256)
	}

	r := bytes.NewReader(data)
	chunkCh, errCh := c.Split(r)

	var chunks []*core.Chunk
	for chunk := range chunkCh {
		chunks = append(chunks, chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	expectedSizes := []uint32{100, 100, 50}
	expectedOffsets := []int64{0, 100, 200}

	for i, chunk := range chunks {
		if chunk.Header.Index != uint32(i) {
			t.Errorf("chunk %d: expected index %d, got %d", i, i, chunk.Header.Index)
		}
		if chunk.Header.Offset != expectedOffsets[i] {
			t.Errorf("chunk %d: expected offset %d, got %d", i, expectedOffsets[i], chunk.Header.Offset)
		}
		if chunk.Header.PlainSize != expectedSizes[i] {
			t.Errorf("chunk %d: expected PlainSize %d, got %d", i, expectedSizes[i], chunk.Header.PlainSize)
		}
		if uint32(len(chunk.Data)) != expectedSizes[i] {
			t.Errorf("chunk %d: expected Data len %d, got %d", i, expectedSizes[i], len(chunk.Data))
		}
		if !bytes.Equal(chunk.Data, data[expectedOffsets[i]:expectedOffsets[i]+int64(expectedSizes[i])]) {
			t.Errorf("chunk %d data mismatch", i)
		}

		// Recycle buffer
		c.PutBuffer(chunk.Data)
	}
}

func TestChunker_EmptyReader(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := NewChunker(config)

	r := bytes.NewReader([]byte{})
	chunkCh, errCh := c.Split(r)

	var count int
	for chunk := range chunkCh {
		count++
		c.PutBuffer(chunk.Data)
	}

	if count != 0 {
		t.Fatalf("expected 0 chunks for empty reader, got %d", count)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error for empty reader: %v", err)
	}
}

type errReader struct{}

func (errReader) Read(p []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestChunker_ReadError(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := NewChunker(config)

	chunkCh, errCh := c.Split(errReader{})

	for chunk := range chunkCh {
		c.PutBuffer(chunk.Data)
	}

	err := <-errCh
	if err == nil {
		t.Fatalf("expected error from errReader, got nil")
	}
}
