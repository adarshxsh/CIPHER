package chunker

import (
	"io"
	"sync"

	"cipher/internal/content/core"
)

const (
	// DefaultChannelCapacity is the default buffer capacity for chunk channels (16-32 chunks).
	DefaultChannelCapacity = 16
	// DefaultChunkSize is used if EngineConfig.ChunkSize is unconfigured (0).
	DefaultChunkSize = 32 * 1024
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config  core.EngineConfig
	pool    sync.Pool
	chanCap int
}

func NewChunker(config core.EngineConfig) *Chunker {
	return NewChunkerWithCapacity(config, DefaultChannelCapacity)
}

func NewChunkerWithCapacity(config core.EngineConfig, chanCap int) *Chunker {
	if config.ChunkSize == 0 {
		config.ChunkSize = DefaultChunkSize
	}
	if chanCap <= 0 {
		chanCap = DefaultChannelCapacity
	}

	chunkSize := config.ChunkSize
	c := &Chunker{
		config:  config,
		chanCap: chanCap,
	}
	c.pool.New = func() any {
		b := make([]byte, chunkSize)
		return &b
	}
	return c
}

// GetBuffer retrieves a byte buffer from the pool, reslicing it to max chunk capacity.
func (c *Chunker) GetBuffer() []byte {
	bp := c.pool.Get().(*[]byte)
	b := *bp
	if uint32(cap(b)) < c.config.ChunkSize {
		b = make([]byte, c.config.ChunkSize)
		*bp = b
	}
	return b[:c.config.ChunkSize]
}

// PutBuffer returns a byte buffer to the chunker's buffer pool for reuse.
func (c *Chunker) PutBuffer(buf []byte) {
	if buf == nil || uint32(cap(buf)) < c.config.ChunkSize {
		return
	}
	buf = buf[:c.config.ChunkSize]
	c.pool.Put(&buf)
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk, c.chanCap)
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
