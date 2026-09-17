package chunker_test

import (
	"bytes"
	"crypto/rand"
	"reflect"
	"testing"
	"unsafe"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_BufferedChannel(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:        1024,
		ChannelBufferCap: 32,
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 50*1024)
	rand.Read(data)

	chunkCh, errCh := c.Split(bytes.NewReader(data))

	// Get channel capacity using reflection
	capVal := reflect.ValueOf(chunkCh).Cap()
	if capVal < 16 {
		t.Errorf("expected channel capacity >= 16, got %d", capVal)
	}
	if capVal != 32 {
		t.Errorf("expected channel capacity 32, got %d", capVal)
	}

	var totalRead int
	for chunk := range chunkCh {
		totalRead += len(chunk.Data)
		c.RecycleChunk(chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if totalRead != len(data) {
		t.Errorf("total read bytes %d != expected %d", totalRead, len(data))
	}
}

func TestChunker_DefaultChannelCap(t *testing.T) {
	// Unspecified ChannelBufferCap (0) should default to DefaultChannelBufferCap (32)
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 2048)
	chunkCh, _ := c.Split(bytes.NewReader(data))

	capVal := reflect.ValueOf(chunkCh).Cap()
	if capVal < 16 {
		t.Errorf("expected default channel capacity >= 16, got %d", capVal)
	}
	if capVal != chunker.DefaultChannelBufferCap {
		t.Errorf("expected channel capacity %d, got %d", chunker.DefaultChannelBufferCap, capVal)
	}

	for chunk := range chunkCh {
		c.RecycleChunk(chunk)
	}
}

func TestChunker_BufferPoolReuse(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := chunker.NewChunker(config)

	// Get a buffer, note backing array address
	buf1 := c.GetBuffer(1024)
	ptr1 := unsafe.SliceData(buf1)

	// Put it back
	c.PutBuffer(buf1)

	// Get buffer again
	buf2 := c.GetBuffer(1024)
	ptr2 := unsafe.SliceData(buf2)

	if ptr1 != ptr2 {
		t.Errorf("expected sync.Pool to reuse buffer memory address (%p != %p)", ptr1, ptr2)
	}
}

func TestChunker_VariableChunkSizes(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 512,
	}
	c := chunker.NewChunker(config)

	// Put a small buffer into pool
	c.PutBuffer(make([]byte, 256))

	// Request larger buffer (1024)
	buf := c.GetBuffer(1024)
	if len(buf) != 1024 {
		t.Errorf("expected len 1024, got %d", len(buf))
	}
	if cap(buf) < 1024 {
		t.Errorf("expected cap >= 1024, got %d", cap(buf))
	}

	// Put a large buffer into pool
	c.PutBuffer(make([]byte, 2048))

	// Request smaller buffer (512)
	buf2 := c.GetBuffer(512)
	if len(buf2) != 512 {
		t.Errorf("expected len 512, got %d", len(buf2))
	}
}

func TestChunker_SplitDataIntegrity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 100,
	}
	c := chunker.NewChunker(config)

	original := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	chunkCh, errCh := c.Split(bytes.NewReader(original))

	var reassembled []byte
	var idx uint32
	var expectedOffset int64

	for chunk := range chunkCh {
		if chunk.Header.Index != idx {
			t.Errorf("expected index %d, got %d", idx, chunk.Header.Index)
		}
		if chunk.Header.Offset != expectedOffset {
			t.Errorf("expected offset %d, got %d", expectedOffset, chunk.Header.Offset)
		}
		if chunk.Header.PlainSize != uint32(len(chunk.Data)) {
			t.Errorf("expected PlainSize %d, got %d", len(chunk.Data), chunk.Header.PlainSize)
		}

		reassembled = append(reassembled, chunk.Data...)
		expectedOffset += int64(len(chunk.Data))
		idx++

		c.RecycleChunk(chunk)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.Equal(original, reassembled) {
		t.Errorf("reassembled data does not match original")
	}
}

func BenchmarkChunker_Split(b *testing.B) {
	config := core.EngineConfig{
		ChunkSize: 32 * 1024,
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 10*1024*1024) // 10MB
	rand.Read(data)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		chunkCh, _ := c.Split(r)
		for chunk := range chunkCh {
			c.RecycleChunk(chunk)
		}
	}
}
