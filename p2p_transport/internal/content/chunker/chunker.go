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
	if config.ChannelBufferSize <= 0 {
		config.ChannelBufferSize = core.DefaultChannelBufferSize
	}
	c := &Chunker{
		config: config,
	}
	chunkSize := int(config.ChunkSize)
	c.pool.New = func() any {
		return make([]byte, chunkSize+64)
	}
	return c
}

// GetBuffer retrieves a byte slice buffer of size ChunkSize from the pool.
func (c *Chunker) GetBuffer() []byte {
	buf := c.pool.Get().([]byte)
	chunkSize := int(c.config.ChunkSize)
	if cap(buf) < chunkSize {
		buf = make([]byte, chunkSize+64)
	}
	return buf[:chunkSize]
}

// PutBuffer returns a byte buffer to the chunker's buffer pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}
	c.pool.Put(buf)
}

// ReleaseChunk returns the chunk's Data buffer to the chunker's buffer pool and zeroes the chunk's Data field.
func (c *Chunker) ReleaseChunk(chunk *core.Chunk) {
	if chunk == nil || chunk.Data == nil {
		return
	}
	c.pool.Put(chunk.Data)
	chunk.Data = nil
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	bufSize := c.config.ChannelBufferSize
	if bufSize <= 0 {
		bufSize = core.DefaultChannelBufferSize
	}

	chunkCh := make(chan *core.Chunk, bufSize)
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
