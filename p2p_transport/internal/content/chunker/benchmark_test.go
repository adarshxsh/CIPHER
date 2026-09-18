package chunker

import (
	"bytes"
	"testing"

	"cipher/internal/content/core"
)

func BenchmarkChunker_Split(b *testing.B) {
	sizes := []struct {
		name      string
		bufCap    int
		chunkSize uint32
	}{
		{"Unbuffered_32K", 0, 32 * 1024},
		{"Buffered16_32K", 16, 32 * 1024},
		{"Buffered64_32K", 64, 32 * 1024},
		{"Buffered128_32K", 128, 32 * 1024},
	}

	totalDataSize := 10 * 1024 * 1024 // 10MB
	data := make([]byte, totalDataSize)

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(totalDataSize))
			config := core.EngineConfig{ChunkSize: tc.chunkSize}
			c := NewChunker(config, WithChannelBuffer(tc.bufCap))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(data)
				chunkCh, errCh := c.Split(r)

				for range chunkCh {
				}
				if err := <-errCh; err != nil {
					b.Fatalf("benchmark failed: %v", err)
				}
			}
		})
	}
}
