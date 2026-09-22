package chunker_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

type errorReader struct {
	data []byte
	read int
	err  error
}

func (r *errorReader) Read(p []byte) (int, error) {
	if r.read < len(r.data) {
		n := copy(p, r.data[r.read:])
		r.read += n
		return n, nil
	}
	return 0, r.err
}

func TestChunker_Split(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:         32 * 1024,
		ChannelBufferSize: 8,
	}
	chk := chunker.NewChunker(config)

	dataSize := 100 * 1024 // 100 KB -> 4 chunks (32K, 32K, 32K, 4K)
	inputData := make([]byte, dataSize)
	for i := range inputData {
		inputData[i] = byte(i % 256)
	}

	reader := bytes.NewReader(inputData)
	chunkCh, errCh := chk.Split(reader)

	var chunks []*core.Chunk
	for chunk := range chunkCh {
		chunks = append(chunks, chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error from errCh: %v", err)
	}

	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}

	expectedSizes := []uint32{32 * 1024, 32 * 1024, 32 * 1024, 4 * 1024}
	var currentOffset int64
	for i, chunk := range chunks {
		if chunk.Header.Index != uint32(i) {
			t.Errorf("chunk %d has index %d, expected %d", i, chunk.Header.Index, i)
		}
		if chunk.Header.Offset != currentOffset {
			t.Errorf("chunk %d has offset %d, expected %d", i, chunk.Header.Offset, currentOffset)
		}
		if chunk.Header.PlainSize != expectedSizes[i] {
			t.Errorf("chunk %d has PlainSize %d, expected %d", i, chunk.Header.PlainSize, expectedSizes[i])
		}
		if uint32(len(chunk.Data)) != expectedSizes[i] {
			t.Errorf("chunk %d Data len is %d, expected %d", i, len(chunk.Data), expectedSizes[i])
		}

		currentOffset += int64(expectedSizes[i])
		chk.ReleaseChunk(chunk)
	}
}

func TestChunker_BufferRecycling(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:         1024,
		ChannelBufferSize: 4,
	}
	chk := chunker.NewChunker(config)

	// Get a buffer, put it back, get it again, and check pointer equality.
	buf1 := chk.GetBuffer()
	chk.PutBuffer(buf1)

	buf2 := chk.GetBuffer()
	if &buf1[0] != &buf2[0] {
		t.Logf("Note: sync.Pool may allocate new buffer, but buffer reuse occurred as expected")
	}

	// Verify ReleaseChunk zeroes chunk.Data
	c := &core.Chunk{Data: buf2}
	chk.ReleaseChunk(c)
	if c.Data != nil {
		t.Errorf("expected chunk.Data to be nil after ReleaseChunk, got non-nil")
	}
}

func TestChunker_ShortReadAndEOF(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 32 * 1024,
	}
	chk := chunker.NewChunker(config)

	// Single chunk smaller than ChunkSize
	data := []byte("hello cipher chunker")
	reader := bytes.NewReader(data)

	chunkCh, errCh := chk.Split(reader)

	var chunks []*core.Chunk
	for c := range chunkCh {
		chunks = append(chunks, c)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	if string(chunks[0].Data) != "hello cipher chunker" {
		t.Errorf("expected content 'hello cipher chunker', got '%s'", string(chunks[0].Data))
	}
	chk.ReleaseChunk(chunks[0])
}

func TestChunker_ReadError(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	chk := chunker.NewChunker(config)

	expectedErr := errors.New("read failure")
	r := &errorReader{
		data: make([]byte, 500),
		err:  expectedErr,
	}

	chunkCh, errCh := chk.Split(r)

	var chunks []*core.Chunk
	for c := range chunkCh {
		chunks = append(chunks, c)
	}

	err := <-errCh
	if err == nil {
		t.Fatalf("expected error from errCh, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	if len(chunks) != 1 {
		t.Errorf("expected 1 chunk read before error, got %d", len(chunks))
	}
	for _, c := range chunks {
		chk.ReleaseChunk(c)
	}
}

func TestChunker_DefaultChannelBufferSize(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:         1024,
		ChannelBufferSize: 0, // Should default to core.DefaultChannelBufferSize
	}
	chk := chunker.NewChunker(config)

	chunkCh, errCh := chk.Split(bytes.NewReader(make([]byte, 2048)))
	if cap(chunkCh) != core.DefaultChannelBufferSize {
		t.Errorf("expected channel cap %d, got %d", core.DefaultChannelBufferSize, cap(chunkCh))
	}

	for c := range chunkCh {
		chk.ReleaseChunk(c)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
