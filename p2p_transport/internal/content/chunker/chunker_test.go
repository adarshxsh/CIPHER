package chunker_test

import (
	"bytes"
	"io"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func TestChunker_DefaultChannelCapacity(t *testing.T) {
	cfg := core.EngineConfig{ChunkSize: 1024}
	chk := chunker.NewChunker(cfg)

	r := bytes.NewReader(make([]byte, 5000))
	chunkCh, _ := chk.Split(r)

	if cap(chunkCh) != chunker.DefaultChannelCap {
		t.Errorf("expected default channel capacity %d, got %d", chunker.DefaultChannelCap, cap(chunkCh))
	}
}

func TestChunker_CustomChannelCapacity(t *testing.T) {
	cfg := core.EngineConfig{ChunkSize: 1024, ChannelCap: 64}
	chk := chunker.NewChunker(cfg)

	r := bytes.NewReader(make([]byte, 5000))
	chunkCh, _ := chk.Split(r)

	if cap(chunkCh) != 64 {
		t.Errorf("expected channel capacity 64, got %d", cap(chunkCh))
	}
}

func TestChunker_BufferRecyclingAndPartialReadSafety(t *testing.T) {
	chunkSize := uint32(1024)
	cfg := core.EngineConfig{ChunkSize: chunkSize, ChannelCap: 16}
	chk := chunker.NewChunker(cfg)

	// Stream of 2500 bytes -> chunks of 1024, 1024, 452 bytes
	data := make([]byte, 2500)
	for i := range data {
		data[i] = byte(i % 256)
	}

	r := bytes.NewReader(data)
	chunkCh, errCh := chk.Split(r)

	var receivedData []byte
	var lastBuffer []byte

	for chunk := range chunkCh {
		receivedData = append(receivedData, chunk.Data...)
		lastBuffer = chunk.Data
		chk.PutBuffer(chunk.Data)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("unexpected split error: %v", err)
	}

	if !bytes.Equal(receivedData, data) {
		t.Errorf("received data mismatch")
	}

	// Verify that after returning partial buffer (452 bytes), GetBuffer returns full ChunkSize
	if len(lastBuffer) != 452 {
		t.Errorf("expected last chunk length 452, got %d", len(lastBuffer))
	}

	reusedBuf := chk.GetBuffer()
	if len(reusedBuf) != int(chunkSize) {
		t.Errorf("expected reused buffer length %d, got %d", chunkSize, len(reusedBuf))
	}
	if cap(reusedBuf) < int(chunkSize) {
		t.Errorf("expected reused buffer capacity >= %d, got %d", chunkSize, cap(reusedBuf))
	}
	chk.PutBuffer(reusedBuf)
}

func BenchmarkChunker_Split(b *testing.B) {
	chunkSize := uint32(32 * 1024)
	cfg := core.EngineConfig{ChunkSize: chunkSize, ChannelCap: 32}
	chk := chunker.NewChunker(cfg)

	data := make([]byte, 10*1024*1024) // 10MB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		chunkCh, errCh := chk.Split(r)

		for chunk := range chunkCh {
			chk.PutBuffer(chunk.Data)
		}

		if err := <-errCh; err != nil && err != io.EOF {
			b.Fatalf("Split error: %v", err)
		}
	}
}
