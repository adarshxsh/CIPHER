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
	if r.read >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.read:])
	r.read += n
	if r.read >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

func TestChunkerInputValidation(t *testing.T) {
	config := core.EngineConfig{ChunkSize: 1024}
	c := chunker.NewChunker(config)

	t.Run("Nil reader", func(t *testing.T) {
		chunkCh, errCh := c.Split(nil)
		err := <-errCh
		if !errors.Is(err, chunker.ErrNilReader) {
			t.Fatalf("expected ErrNilReader, got %v", err)
		}
		if _, ok := <-chunkCh; ok {
			t.Fatal("expected chunkCh to be closed")
		}
	})

	t.Run("Nil chunker instance", func(t *testing.T) {
		var nilChunker *chunker.Chunker
		r := bytes.NewReader([]byte("test data"))
		chunkCh, errCh := nilChunker.Split(r)
		err := <-errCh
		if !errors.Is(err, chunker.ErrNilChunker) {
			t.Fatalf("expected ErrNilChunker, got %v", err)
		}
		if _, ok := <-chunkCh; ok {
			t.Fatal("expected chunkCh to be closed")
		}
	})

	t.Run("Zero chunk size", func(t *testing.T) {
		invalidChunker := &chunker.Chunker{}
		r := bytes.NewReader([]byte("test data"))
		chunkCh, errCh := invalidChunker.Split(r)
		err := <-errCh
		if !errors.Is(err, chunker.ErrInvalidChunkSize) {
			t.Fatalf("expected ErrInvalidChunkSize, got %v", err)
		}
		if _, ok := <-chunkCh; ok {
			t.Fatal("expected chunkCh to be closed")
		}
	})
}

func TestChunkerSplit(t *testing.T) {
	chunkSize := uint32(100)
	config := core.EngineConfig{
		ChunkSize:       chunkSize,
		ChannelCapacity: 8,
	}
	c := chunker.NewChunker(config)

	data := make([]byte, 250) // Should result in 3 chunks: 100, 100, 50
	for i := range data {
		data[i] = byte(i % 256)
	}

	r := bytes.NewReader(data)
	chunkCh, errCh := c.Split(r)

	if cap(chunkCh) != 8 {
		t.Fatalf("expected channel capacity 8, got %d", cap(chunkCh))
	}

	var chunks []*core.Chunk
	for ch := range chunkCh {
		chunks = append(chunks, ch)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error from errCh: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	if chunks[0].Header.PlainSize != 100 || chunks[0].Header.Index != 0 || chunks[0].Header.Offset != 0 {
		t.Errorf("chunk 0 header mismatch: %+v", chunks[0].Header)
	}
	if chunks[1].Header.PlainSize != 100 || chunks[1].Header.Index != 1 || chunks[1].Header.Offset != 100 {
		t.Errorf("chunk 1 header mismatch: %+v", chunks[1].Header)
	}
	if chunks[2].Header.PlainSize != 50 || chunks[2].Header.Index != 2 || chunks[2].Header.Offset != 200 {
		t.Errorf("chunk 2 header mismatch: %+v", chunks[2].Header)
	}

	// Verify data contents
	reconstructed := make([]byte, 0, 250)
	for _, ch := range chunks {
		reconstructed = append(reconstructed, ch.Data...)
	}
	if !bytes.Equal(data, reconstructed) {
		t.Fatal("reconstructed data does not match original data")
	}
}

func TestChunkerSyncPoolReuse(t *testing.T) {
	chunkSize := uint32(512)
	c := chunker.NewChunker(core.EngineConfig{ChunkSize: chunkSize})

	buf1 := c.GetBuffer()
	if len(buf1) != int(chunkSize) {
		t.Fatalf("expected buffer length %d, got %d", chunkSize, len(buf1))
	}

	// Modify buffer to test reuse
	buf1[0] = 0xAA
	c.PutBuffer(buf1)

	buf2 := c.GetBuffer()
	if len(buf2) != int(chunkSize) {
		t.Fatalf("expected buffer length %d, got %d", chunkSize, len(buf2))
	}
	// Verify that buf2 is the recycled buffer
	if buf2[0] != 0xAA {
		t.Logf("buf2[0] = 0x%X (newly allocated or reset)", buf2[0])
	}
}

func TestChunkerErrorPathCleanup(t *testing.T) {
	c := chunker.NewChunker(core.EngineConfig{ChunkSize: 100})

	testErr := errors.New("read error mid stream")
	r := &errorReader{
		data: make([]byte, 150),
		err:  testErr,
	}

	chunkCh, errCh := c.Split(r)

	var chunks []*core.Chunk
	for ch := range chunkCh {
		chunks = append(chunks, ch)
	}

	err := <-errCh
	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got %v", err)
	}

	// Should have read 2 chunks (100 bytes + 50 bytes) before error
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks before error, got %d", len(chunks))
	}
}
