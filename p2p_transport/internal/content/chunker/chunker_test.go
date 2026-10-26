package chunker

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
)

type errReader struct {
	err error
}

func (r *errReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func TestChunkerSplitChannelBuffered(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	c := NewChunker(config)
	r := bytes.NewReader([]byte("test data"))

	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != 16 {
		t.Fatalf("expected chunkCh capacity to be 16, got %d", cap(chunkCh))
	}

	for range chunkCh {
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkerSplitData(t *testing.T) {
	chunkSize := uint32(1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := NewChunker(config)

	// Create 2.5 chunks worth of data (2560 bytes)
	data := make([]byte, 2560)
	for i := range data {
		data[i] = byte(i % 251)
	}

	r := bytes.NewReader(data)
	chunkCh, errCh := c.Split(r)

	var reassembled []byte
	var count uint32

	for chunk := range chunkCh {
		if chunk.Header.Index != count {
			t.Errorf("expected index %d, got %d", count, chunk.Header.Index)
		}
		expectedOffset := int64(count) * int64(chunkSize)
		if chunk.Header.Offset != expectedOffset {
			t.Errorf("expected offset %d, got %d", expectedOffset, chunk.Header.Offset)
		}
		if chunk.Header.Version != 1 {
			t.Errorf("expected version 1, got %d", chunk.Header.Version)
		}

		reassembled = append(reassembled, chunk.Data...)
		count++

		// Test recycling chunk back to buffer pool
		RecycleChunk(chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 3 {
		t.Fatalf("expected 3 chunks, got %d", count)
	}

	if !bytes.Equal(reassembled, data) {
		t.Fatal("reassembled data does not match original input")
	}
}

func TestBufferPoolRecycling(t *testing.T) {
	chunkSize := uint32(512)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := NewChunker(config)

	// Run multiple splits sequentially to verify buffer reuse from sync.Pool
	for run := 0; run < 10; run++ {
		data := bytes.Repeat([]byte{byte(run + 1)}, 2048)
		r := bytes.NewReader(data)
		chunkCh, errCh := c.Split(r)

		var chunks []*core.Chunk
		for chunk := range chunkCh {
			chunks = append(chunks, chunk)
		}

		if err := <-errCh; err != nil {
			t.Fatalf("run %d failed with error: %v", run, err)
		}

		if len(chunks) != 4 {
			t.Fatalf("run %d expected 4 chunks, got %d", run, len(chunks))
		}

		for _, ch := range chunks {
			if len(ch.Data) != int(chunkSize) {
				t.Fatalf("unexpected chunk data size: %d", len(ch.Data))
			}
			// Recycle buffers after reading
			PutBuffer(ch.Data)
		}
	}
}

func TestChunkerSplitEmpty(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	c := NewChunker(config)

	r := bytes.NewReader(nil)
	chunkCh, errCh := c.Split(r)

	chunkCount := 0
	for range chunkCh {
		chunkCount++
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if chunkCount != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", chunkCount)
	}
}

func TestChunkerSplitError(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	c := NewChunker(config)

	expectedErr := errors.New("read error")
	r := &errReader{err: expectedErr}

	chunkCh, errCh := c.Split(r)

	for range chunkCh {
	}

	err := <-errCh
	if err == nil || !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func BenchmarkChunkerSplit(b *testing.B) {
	chunkSize := uint32(256 * 1024)
	config := core.EngineConfig{ChunkSize: chunkSize}
	c := NewChunker(config)

	data := make([]byte, 10*1024*1024) // 10MB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		chunkCh, errCh := c.Split(r)

		for chunk := range chunkCh {
			RecycleChunk(chunk)
		}

		if err := <-errCh; err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
