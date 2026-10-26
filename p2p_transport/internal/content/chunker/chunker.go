package chunker

import (
	"io"

	"cipher/internal/content/core"
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
	pool   core.BufferPool
}

func NewChunker(config core.EngineConfig, pool core.BufferPool) *Chunker {
	return &Chunker{
		config: config,
		pool:   pool,
	}
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			var buf []byte
			if c.pool != nil {
				buf = c.pool.Get()
				if cap(buf) < int(c.config.ChunkSize) {
					buf = make([]byte, c.config.ChunkSize)
				} else {
					buf = buf[:c.config.ChunkSize]
				}
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
