package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const DefaultChannelCap = 32

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config     core.EngineConfig
	bufferPool *sync.Pool
}

func NewChunker(config core.EngineConfig) *Chunker {
	if config.ChannelCap <= 0 {
		config.ChannelCap = DefaultChannelCap
	}
	chunkSize := config.ChunkSize
	return &Chunker{
		config: config,
		bufferPool: &sync.Pool{
			New: func() any {
				buf := make([]byte, chunkSize, chunkSize+16)
				return &buf
			},
		},
	}
}

// GetBuffer retrieves a byte buffer from the pool, resliced to ChunkSize.
func (c *Chunker) GetBuffer() []byte {
	v := c.bufferPool.Get()
	if v == nil {
		return make([]byte, c.config.ChunkSize, c.config.ChunkSize+16)
	}
	bufPtr := v.(*[]byte)
	buf := *bufPtr
	if cap(buf) < int(c.config.ChunkSize) {
		return make([]byte, c.config.ChunkSize, c.config.ChunkSize+16)
	}
	return buf[:c.config.ChunkSize]
}

// PutBuffer returns a byte buffer back to the pool for reuse.
func (c *Chunker) PutBuffer(buf []byte) {
	if cap(buf) < int(c.config.ChunkSize) {
		return
	}
	fullBuf := buf[:cap(buf)]
	c.bufferPool.Put(&fullBuf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chanCap := c.config.ChannelCap
	if chanCap <= 0 {
		chanCap = DefaultChannelCap
	}
	chunkCh := make(chan *core.Chunk, chanCap)
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
