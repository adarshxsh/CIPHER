package chunker

import (
	"errors"
	"io"
	"sync"

	"cipher/internal/content/core"
)

// DefaultChannelCapacity is the default buffer capacity for chunk channels.
const DefaultChannelCapacity = 16

var (
	// ErrNilReader is returned when a nil reader is supplied to Split.
	ErrNilReader = errors.New("chunker: reader is nil")
	// ErrInvalidChunkSize is returned when chunk size is zero or invalid.
	ErrInvalidChunkSize = errors.New("chunker: invalid chunk size")
	// ErrNilChunker is returned when Split is called on a nil Chunker instance.
	ErrNilChunker = errors.New("chunker: nil chunker instance")
)

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config core.EngineConfig
	pool   sync.Pool
}

// NewChunker initializes a new Chunker with sync.Pool buffer allocation.
func NewChunker(config core.EngineConfig) *Chunker {
	if config.ChunkSize == 0 {
		config.ChunkSize = 32 * 1024 // 32KB default
	}

	chunkSize := config.ChunkSize
	return &Chunker{
		config: config,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, chunkSize)
				return &b
			},
		},
	}
}

// GetBuffer retrieves a chunk-sized byte slice from the sync.Pool.
func (c *Chunker) GetBuffer() []byte {
	if c == nil {
		return nil
	}
	bufPtr, ok := c.pool.Get().(*[]byte)
	if !ok || bufPtr == nil {
		chunkSize := c.config.ChunkSize
		if chunkSize == 0 {
			chunkSize = 32 * 1024
		}
		b := make([]byte, chunkSize)
		return b
	}
	b := *bufPtr
	if uint32(cap(b)) < c.config.ChunkSize {
		b = make([]byte, c.config.ChunkSize)
	} else {
		b = b[:c.config.ChunkSize]
	}
	return b
}

// PutBuffer returns a byte slice buffer to the sync.Pool.
func (c *Chunker) PutBuffer(buf []byte) {
	if c == nil || buf == nil {
		return
	}
	chunkSize := c.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = 32 * 1024
	}
	if uint32(cap(buf)) < chunkSize {
		return
	}
	buf = buf[:chunkSize]
	c.pool.Put(&buf)
}

func (c *Chunker) channelCapacity() int {
	if c != nil && c.config.ChannelCapacity > 0 {
		return c.config.ChannelCapacity
	}
	return DefaultChannelCapacity
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	if c == nil {
		chunkCh := make(chan *core.Chunk, DefaultChannelCapacity)
		errCh := make(chan error, 1)
		errCh <- ErrNilChunker
		close(chunkCh)
		close(errCh)
		return chunkCh, errCh
	}

	capacity := c.channelCapacity()

	// Validate input parameters before resource allocation
	if r == nil {
		chunkCh := make(chan *core.Chunk, capacity)
		errCh := make(chan error, 1)
		errCh <- ErrNilReader
		close(chunkCh)
		close(errCh)
		return chunkCh, errCh
	}

	if c.config.ChunkSize == 0 {
		chunkCh := make(chan *core.Chunk, capacity)
		errCh := make(chan error, 1)
		errCh <- ErrInvalidChunkSize
		close(chunkCh)
		close(errCh)
		return chunkCh, errCh
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
				// Clean up allocated buffer if no bytes were read
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
