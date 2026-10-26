package chunker_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"cipher/internal/content/chunker"
	"cipher/internal/content/core"
)

func BenchmarkChunker_Split(b *testing.B) {
	chunkSize := uint32(256 * 1024) // 256KB chunks
	dataSize := 10 * 1024 * 1024    // 10MB file
	data := make([]byte, dataSize)
	rand.Read(data)

	config := core.EngineConfig{
		ChunkSize:        chunkSize,
		ChannelBufferCap: 64,
	}

	c := chunker.NewChunker(config)

	b.SetBytes(int64(dataSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		chunkCh, errCh := c.Split(reader)

		for chunk := range chunkCh {
			c.PutBuffer(chunk.Data)
		}

		if err := <-errCh; err != nil {
			b.Fatalf("Split error: %v", err)
		}
	}
}
