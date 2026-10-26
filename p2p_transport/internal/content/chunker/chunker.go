package chunker

import (
	"io"

	"cipher/internal/content/core"
)

// DefaultChannelBuffer is the default buffer capacity for chunk emission channels.
const DefaultChannelBuffer = 64

// DefaultChunkSize is the default chunk size in bytes if unconfigured.
const DefaultChunkSize = 32 * 1024

// Option represents a configuration option for Chunker.
type Option func(*Chunker)

// WithChannelBuffer returns an Option that configures the channel buffer capacity for chunk emission.
func WithChannelBuffer(capacity int) Option {
	return func(c *Chunker) {
		if capacity > 0 {
			c.channelBuffer = capacity
		}
	}
}

// Chunker is responsible for splitting a stream into Chunks.
type Chunker struct {
	config        core.EngineConfig
	channelBuffer int
}

// NewChunker creates a new Chunker instance with the provided engine config and optional options.
func NewChunker(config core.EngineConfig, opts ...Option) *Chunker {
	c := &Chunker{
		config:        config,
		channelBuffer: DefaultChannelBuffer,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Split reads from r and emits chunks on the returned channel.
// It closes the channel and returns any read error (other than EOF).
func (c *Chunker) Split(r io.Reader) (<-chan *core.Chunk, <-chan error) {
	bufCap := c.channelBuffer
	if bufCap <= 0 {
		bufCap = DefaultChannelBuffer
	}

	chunkCh := make(chan *core.Chunk, bufCap)
	errCh := make(chan error, 1)

	chunkSize := c.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var index uint32
		var offset int64

		for {
			buf := make([]byte, chunkSize)
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

