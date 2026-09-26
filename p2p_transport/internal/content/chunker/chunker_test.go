package chunker_test

import (
	"bytes"
	"errors"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

type errReader struct {
	data      []byte
	readBytes int
	errAfter  int
	errToReturn error
}

func (r *errReader) Read(p []byte) (n int, err error) {
	if r.readBytes >= r.errAfter {
		return 0, r.errToReturn
	}
	n = copy(p, r.data[r.readBytes:])
	r.readBytes += n
	if r.readBytes >= r.errAfter {
		return n, r.errToReturn
	}
	return n, nil
}

func TestChunker_DefaultChannelCapacity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: 0, // Unconfigured
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 5*1024)
	r := bytes.NewReader(data)
	chunkCh, _ := c.Split(r)

	// Verify channel capacity is defaulted to 16
	if cap(chunkCh) != 16 {
		t.Errorf("expected channel capacity 16, got %d", cap(chunkCh))
	}
}

func TestChunker_CustomChannelCapacity(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: 32,
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 5*1024)
	r := bytes.NewReader(data)
	chunkCh, _ := c.Split(r)

	if cap(chunkCh) != 32 {
		t.Errorf("expected channel capacity 32, got %d", cap(chunkCh))
	}
}

func TestChunker_GetPutBuffer(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := chunker.NewChunker(config)

	buf1 := c.GetBuffer()
	if len(buf1) != 1024 {
		t.Fatalf("expected buffer length 1024, got %d", len(buf1))
	}

	// Put buffer back
	c.PutBuffer(buf1)

	buf2 := c.GetBuffer()
	if len(buf2) != 1024 {
		t.Fatalf("expected buffer length 1024, got %d", len(buf2))
	}
}

func TestChunker_RejectInvalidCapacityBuffer(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize: 1024,
	}
	c := chunker.NewChunker(config)

	// Buffer with invalid small capacity
	smallBuf := make([]byte, 512)
	c.PutBuffer(smallBuf) // Should be ignored gracefully without panic

	fetchedBuf := c.GetBuffer()
	if len(fetchedBuf) != 1024 {
		t.Fatalf("expected buffer length 1024, got %d", len(fetchedBuf))
	}
}

func TestChunker_SplitAndRecycle(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: 16,
	}
	c := chunker.NewChunker(config)

	data := bytes.Repeat([]byte("A"), 2500) // ~2.5 chunks
	r := bytes.NewReader(data)

	chunkCh, errCh := c.Split(r)

	var count int
	var totalBytes int
	for chunk := range chunkCh {
		count++
		totalBytes += len(chunk.Data)
		c.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected split error: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 chunks, got %d", count)
	}
	if totalBytes != len(data) {
		t.Errorf("expected %d total bytes, got %d", len(data), totalBytes)
	}
}

func TestChunker_StreamError_ReturnsUnsubmittedBuffer(t *testing.T) {
	config := core.EngineConfig{
		ChunkSize:       1024,
		ChannelCapacity: 16,
	}
	c := chunker.NewChunker(config)

	expectedErr := errors.New("read failure")
	reader := &errReader{
		data:        make([]byte, 512),
		errAfter:    0,
		errToReturn: expectedErr,
	}

	chunkCh, errCh := c.Split(reader)

	for chunk := range chunkCh {
		c.PutBuffer(chunk.Data)
	}

	err := <-errCh
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestChunker_ConcurrentInstances(t *testing.T) {
	config1 := core.EngineConfig{ChunkSize: 512, ChannelCapacity: 8}
	config2 := core.EngineConfig{ChunkSize: 2048, ChannelCapacity: 16}

	c1 := chunker.NewChunker(config1)
	c2 := chunker.NewChunker(config2)

	done := make(chan bool)

	go func() {
		buf1 := c1.GetBuffer()
		if len(buf1) != 512 {
			t.Errorf("c1 buf length error: %d", len(buf1))
		}
		c1.PutBuffer(buf1)
		done <- true
	}()

	go func() {
		buf2 := c2.GetBuffer()
		if len(buf2) != 2048 {
			t.Errorf("c2 buf length error: %d", len(buf2))
		}
		c2.PutBuffer(buf2)
		done <- true
	}()

	<-done
	<-done
}
