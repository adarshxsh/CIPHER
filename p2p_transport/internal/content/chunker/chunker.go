package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0)
	},
}

// PutBuffer puts a byte buffer back into the package buffer pool for reuse.
func PutBuffer(buf []byte) {
	if buf != nil {
		bufferPool.Put(buf)
	}
}

// RecycleChunk puts the chunk's data buffer back into the package buffer pool.
func RecycleChunk(chunk *core.Chunk) {
	if chunk != nil && chunk.Data != nil {
		bufferPool.Put(chunk.Data)
		chunk.Data = nil
	}
}

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
}

func NewChunker(config core.EngineConfig) *Chunker {
	return &Chunker{
		config: config,
	}
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			var buf []byte
			if b, ok := bufferPool.Get().([]byte); ok && cap(b) >= int(c.config.ChunkSize) {
				buf = b[:c.config.ChunkSize]
			} else {
				buf = make([]byte, c.config.ChunkSize)
			}

			n, err := io.ReadFull(r, buf)

			if n > 0 {
				chunk := &core.Chunk{
					Header: core.ChunkHeader{
						Version:   1, // Chunk metadata version
						Index:     index,
						Offset:    offset,
						PlainSize: uint32(n),
					},
					Data: buf[:n],
				}
				chunkCh <- chunk

				index++
				offset += int64(n)
			} else {
				bufferPool.Put(buf)
			}

			if err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					errCh <- err
				}
				break
			}
		}
	}()

	return chunkCh, errCh
}
