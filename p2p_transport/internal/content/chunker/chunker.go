package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const DefaultChannelBufferCap = 32

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
		chunkSize := int(c.config.ChunkSize)
		if chunkSize <= 0 {
			chunkSize = 32 * 1024
		}
		buf := make([]byte, chunkSize, chunkSize+64)
		return &buf
	}
	return c
}

// GetBuffer retrieves a byte slice from the pool with capacity at least size.
func (c *Chunker) GetBuffer(size int) []byte {
	v := c.pool.Get()
	if v != nil {
		bufPtr := v.(*[]byte)
		buf := *bufPtr
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size, size+64)
}

// PutBuffer recycles a byte slice back to the buffer pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	buf = buf[:cap(buf)]
	c.pool.Put(&buf)
}

// RecycleChunk returns the data buffer of a Chunk back to the pool and resets chunk.Data.
func (c *Chunker) RecycleChunk(chunk *core.Chunk) {
	if chunk != nil && chunk.Data != nil {
		c.PutBuffer(chunk.Data)
		chunk.Data = nil
	}
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chanCap := c.config.ChannelBufferCap
	if chanCap <= 0 {
		chanCap = DefaultChannelBufferCap
	}

	chunkCh := make(chan *core.Chunk, chanCap)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		chunkSize := int(c.config.ChunkSize)
		if chunkSize <= 0 {
			chunkSize = 32 * 1024
		}

		for {
			buf := c.GetBuffer(chunkSize)
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
