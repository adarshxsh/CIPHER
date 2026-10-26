package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const (
	// DefaultChannelCapacity is the channel capacity for chunk streaming channels.
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
	c.pool = sync.Pool{
		New: func() any {
			// Allocate extra capacity (64 bytes) beyond ChunkSize to accommodate
			// encryption overhead (e.g. Poly1305 auth tag) for zero-allocation in-place encryption.
			return make([]byte, config.ChunkSize+64)
		},
	}
	return c
}

// GetBuffer retrieves a byte slice from the buffer pool.
func (c *Chunker) GetBuffer() []byte {
	buf := c.pool.Get().([]byte)
	if uint32(cap(buf)) < c.config.ChunkSize {
		buf = make([]byte, c.config.ChunkSize+64)
	}
	return buf[:c.config.ChunkSize]
}

// PutBuffer resets/clears a byte slice buffer and returns it to the buffer pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	// Reset/clear buffer contents before reuse
	fullBuf := buf[:cap(buf)]
	for i := range fullBuf {
		fullBuf[i] = 0
	}
	c.pool.Put(fullBuf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk, DefaultChannelCapacity)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			buf := c.GetBuffer()
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
