package chunker

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/core"
)

func TestChunker_DefaultChannelBuffer(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	c := NewChunker(config)

	r := bytes.NewReader(make([]byte, 100))
	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != DefaultChannelBuffer {
		t.Fatalf("expected chunk channel buffer capacity %d, got %d", DefaultChannelBuffer, cap(chunkCh))
	}

	for range chunkCh {
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_WithChannelBuffer(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 32 * 1024}

	customCap := 128
	c := NewChunker(config, WithChannelBuffer(customCap))

	r := bytes.NewReader(make([]byte, 100))
	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != customCap {
		t.Fatalf("expected custom chunk channel buffer capacity %d, got %d", customCap, cap(chunkCh))
	}

	for range chunkCh {
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_ZeroValueFallback(t *testing.T) {
	// Test zero struct initialization
	cZero := &Chunker{}
	r1 := bytes.NewReader(make([]byte, 100))
	chunkCh1, errCh1 := cZero.Split(r1)
	if cap(chunkCh1) != DefaultChannelBuffer {
		t.Fatalf("expected fallback buffer capacity %d, got %d", DefaultChannelBuffer, cap(chunkCh1))
	}
	for range chunkCh1 {
	}
	if err := <-errCh1; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test invalid WithChannelBuffer capacity <= 0
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	cInvalid := NewChunker(config, WithChannelBuffer(-10))
	r2 := bytes.NewReader(make([]byte, 100))
	chunkCh2, errCh2 := cInvalid.Split(r2)
	if cap(chunkCh2) != DefaultChannelBuffer {
		t.Fatalf("expected fallback buffer capacity %d, got %d", DefaultChannelBuffer, cap(chunkCh2))
	}
	for range chunkCh2 {
	}
	if err := <-errCh2; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunker_Split_DataIntegrity(t *testing.T) {
	chunkSize := uint32(1024)
	totalBytes := 2500
	data := make([]byte, totalBytes)
	for i := range data {
		data[i] = byte(i % 256)
	}

	config := core.EngineConfig{ChunkSize: chunkSize}
	c := NewChunker(config, WithChannelBuffer(16))

	r := bytes.NewReader(data)
	chunkCh, errCh := c.Split(r)

	var reassembled []byte
	var expectedIndex uint32
	var expectedOffset int64

	for chunk := range chunkCh {
		if chunk.Header.Index != expectedIndex {
			t.Errorf("expected chunk index %d, got %d", expectedIndex, chunk.Header.Index)
		}
		if chunk.Header.Offset != expectedOffset {
			t.Errorf("expected chunk offset %d, got %d", expectedOffset, chunk.Header.Offset)
		}
		if chunk.Header.Version != 1 {
			t.Errorf("expected chunk version 1, got %d", chunk.Header.Version)
		}
		if uint32(len(chunk.Data)) != chunk.Header.PlainSize {
			t.Errorf("expected plain size %d, got %d", len(chunk.Data), chunk.Header.PlainSize)
		}

		reassembled = append(reassembled, chunk.Data...)
		expectedIndex++
		expectedOffset += int64(len(chunk.Data))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error on errCh: %v", err)
	}

	if !bytes.Equal(data, reassembled) {
		t.Fatalf("reassembled data does not match original data")
	}

	// Verify channel closure (reading from closed channels returns zero value immediately)
	_, open1 := <-chunkCh
	if open1 {
		t.Errorf("expected chunkCh to be closed")
	}
	_, open2 := <-errCh
	if open2 {
		t.Errorf("expected errCh to be closed")
	}
}

type errReader struct {
	readErr error
}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, e.readErr
}

func TestChunker_Split_ErrorHandling(t *testing.T) {
	dummyErr := errors.New("read failed")
	r := &errReader{readErr: dummyErr}

	config := core.EngineConfig{ChunkSize: 1024}
	c := NewChunker(config)

	chunkCh, errCh := c.Split(r)

	// Consume any chunks
	for range chunkCh {
	}

	err := <-errCh
	if !errors.Is(err, dummyErr) {
		t.Fatalf("expected error %v, got %v", dummyErr, err)
	}

	// Verify channels are cleanly closed
	_, open1 := <-chunkCh
	if open1 {
		t.Errorf("expected chunkCh to be closed")
	}
	_, open2 := <-errCh
	if open2 {
		t.Errorf("expected errCh to be closed")
	}
}
