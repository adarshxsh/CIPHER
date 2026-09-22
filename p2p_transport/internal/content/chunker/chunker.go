package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
	pool   sync.Pool
}

func NewChunker(config core.EngineConfig) *Chunker {
	if config.ChunkSize == 0 {
		config.ChunkSize = 32 * 1024
	}
	c := &Chunker{
		config: config,
	}
	c.pool.New = func() any {
		return make([]byte, c.config.ChunkSize)
	}
	return c
}

// GetBuffer retrieves a byte slice buffer from the pool if size matches ChunkSize,
// or allocates a new slice if non-standard size.
func (c *Chunker) GetBuffer(size uint32) []byte {
	if size != c.config.ChunkSize {
		return make([]byte, size)
	}
	buf := c.pool.Get().([]byte)
	return buf[:size]
}

// PutBuffer returns a byte slice buffer to the pool if its capacity matches ChunkSize.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil || cap(buf) != int(c.config.ChunkSize) {
		return
	}
	fullBuf := buf[:c.config.ChunkSize]
	clear(fullBuf)
	c.pool.Put(fullBuf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	bufCap := c.config.ChannelBufferCap
	if bufCap <= 0 {
		bufCap = core.DefaultChannelBufferCap
	}

	chunkCh := make(chan *core.Chunk, bufCap)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			buf := c.GetBuffer(c.config.ChunkSize)
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
				c.PutBuffer(buf)
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
