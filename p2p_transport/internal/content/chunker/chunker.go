package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config     core.EngineConfig
	bufferPool sync.Pool
}

func NewChunker(config core.EngineConfig) *Chunker {
	if config.ChunkSize == 0 {
		config.ChunkSize = 32 * 1024
	}
	if config.ChannelCapacity <= 0 {
		config.ChannelCapacity = 16
	}

	chunkSize := int(config.ChunkSize)
	bufCap := chunkSize + 16 // Reserve capacity for AEAD encryption tag overhead
	c := &Chunker{
		config: config,
	}

	c.bufferPool.New = func() any {
		buf := make([]byte, chunkSize, bufCap)
		return &buf
	}

	return c
}

// GetBuffer retrieves a byte buffer matching config.ChunkSize from the pool.
func (c *Chunker) GetBuffer() []byte {
	bufPtr := c.bufferPool.Get().(*[]byte)
	return (*bufPtr)[:c.config.ChunkSize]
}

// PutBuffer returns a byte buffer matching config.ChunkSize to the pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	expectedCap := int(c.config.ChunkSize) + 16
	if cap(buf) < expectedCap {
		return
	}
	buf = buf[:c.config.ChunkSize:expectedCap]
	c.bufferPool.Put(&buf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	capacity := c.config.ChannelCapacity
	if capacity <= 0 {
		capacity = 16
	}

	chunkCh := make(chan *core.Chunk, capacity)
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

