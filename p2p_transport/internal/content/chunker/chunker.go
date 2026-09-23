package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const DefaultChannelCapacity = 16

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
	pool   *sync.Pool
	once   sync.Once
}

func NewChunker(config core.EngineConfig) *Chunker {
	c := &Chunker{
		config: config,
	}
	c.initPool()
	return c
}

func (c *Chunker) initPool() {
	c.once.Do(func() {
		if c.pool == nil {
			chunkSize := c.config.ChunkSize
			if chunkSize == 0 {
				chunkSize = 32 * 1024
			}
			// Buffer capacity padded with 16 bytes overhead for ChaCha20-Poly1305 AEAD tag
			bufCap := int(chunkSize) + 16
			c.pool = &sync.Pool{
				New: func() interface{} {
					return make([]byte, chunkSize, bufCap)
				},
			}
		}
	})
}

// GetBuffer retrieves a reusable byte buffer from the pool.
func (c *Chunker) GetBuffer() []byte {
	c.initPool()
	buf := c.pool.Get().([]byte)
	chunkSize := c.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = 32 * 1024
	}
	return buf[:chunkSize]
}

// PutBuffer returns a byte buffer back to the memory pool for recycling.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	chunkSize := c.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = 32 * 1024
	}
	if cap(buf) < int(chunkSize) {
		return
	}
	c.initPool()
	c.pool.Put(buf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	cap := c.config.ChannelCapacity
	if cap <= 0 {
		cap = DefaultChannelCapacity
	}

	chunkCh := make(chan *core.Chunk, cap)
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
