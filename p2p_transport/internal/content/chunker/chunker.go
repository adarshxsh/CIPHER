package chunker

import (
	"io"

	"cipher/internal/content/core"
	pool "github.com/libp2p/go-buffer-pool"
)

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
	bufSize := c.config.ChannelBufferSize
	if bufSize <= 0 {
		bufSize = 16
	}
	chunkCh := make(chan *core.Chunk, bufSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			buf := pool.Get(int(c.config.ChunkSize))
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
				pool.Put(buf)
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
