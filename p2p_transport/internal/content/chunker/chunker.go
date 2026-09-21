package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const (
	// DefaultChannelCapacity is the default buffer size for the chunk channel.
	DefaultChannelCapacity = 16
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
	pool   sync.Pool
}

func NewChunker(config core.EngineConfig) *Chunker {
	c := &Chunker{
		config: config,
	}
	c.pool.New = func() any {
		// Include 64-byte headroom for in-place AEAD encryption tags and headers
		return make([]byte, config.ChunkSize+64)
	}
	return c
}

// GetBuffer retrieves a byte buffer from the pool.
func (c *Chunker) GetBuffer() []byte {
	buf := c.pool.Get().([]byte)
	if cap(buf) < int(c.config.ChunkSize) {
		return make([]byte, c.config.ChunkSize+64)
	}
	return buf[:cap(buf)]
}

// PutBuffer returns a byte buffer back to the pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) < int(c.config.ChunkSize) {
		return
	}
	c.pool.Put(buf[:cap(buf)])
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	capSize := c.config.ChannelCapacity
	if capSize <= 0 {
		capSize = DefaultChannelCapacity
	}

	chunkCh := make(chan *core.Chunk, capSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			buf := c.GetBuffer()
			readBuf := buf[:c.config.ChunkSize]
			n, err := io.ReadFull(r, readBuf)

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
