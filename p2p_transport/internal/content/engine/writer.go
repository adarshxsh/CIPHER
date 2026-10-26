package engine

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// byteBufferWriterAt adapts a *bytes.Buffer to implement io.WriterAt for pre-allocated buffers.
type byteBufferWriterAt struct {
	buf *bytes.Buffer
}

func (b *byteBufferWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	data := b.buf.Bytes()
	if off < 0 || off > int64(len(data)) {
		return 0, io.ErrUnexpectedEOF
	}
	if int64(len(p)) > int64(len(data))-off {
		return 0, io.ErrShortWrite
	}
	return copy(data[off:], p), nil
}

// StreamWriter manages sparse, direct-offset file writing and completion tracking during reassembly.
type StreamWriter struct {
	w          io.Writer
	wAt        io.WriterAt
	totalSize  uint64
	chunkCount int
	written    map[uint32]bool
	mu         sync.Mutex
}

// NewStreamWriter initializes a StreamWriter for target file/writer w.
// It pre-allocates target file size if supported (e.g. *os.File or *bytes.Buffer).
func NewStreamWriter(w io.Writer, totalSize uint64, chunkCount int) (*StreamWriter, error) {
	sw := &StreamWriter{
		w:          w,
		totalSize:  totalSize,
		chunkCount: chunkCount,
		written:    make(map[uint32]bool, chunkCount),
	}

	// Case 1: w implements io.WriterAt (e.g. *os.File)
	if wAt, ok := w.(io.WriterAt); ok {
		sw.wAt = wAt
		// Pre-allocate destination file if w supports Truncate (e.g. *os.File)
		if trunc, ok := w.(interface{ Truncate(int64) error }); ok {
			if err := trunc.Truncate(int64(totalSize)); err != nil {
				return nil, fmt.Errorf("failed to pre-allocate destination file: %w", err)
			}
		}
	} else if buf, ok := w.(*bytes.Buffer); ok {
		// Case 2: w is a *bytes.Buffer
		if totalSize > 0 {
			// Grow and pre-allocate zero bytes in buffer
			buf.Grow(int(totalSize))
			zeroes := make([]byte, totalSize)
			buf.Write(zeroes)
			sw.wAt = &byteBufferWriterAt{buf: buf}
		}
	}

	return sw, nil
}

// WriteChunk writes decrypted chunk bytes directly to calculated byte offset and updates completion bitset/map.
func (sw *StreamWriter) WriteChunk(index uint32, offset int64, data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if int(index) >= sw.chunkCount && sw.chunkCount > 0 {
		return fmt.Errorf("chunk index %d out of bounds (max %d)", index, sw.chunkCount)
	}

	if offset < 0 || uint64(offset)+uint64(len(data)) > sw.totalSize {
		return fmt.Errorf("chunk offset %d + len %d exceeds total size %d", offset, len(data), sw.totalSize)
	}

	if sw.wAt != nil {
		n, err := sw.wAt.WriteAt(data, offset)
		if err != nil {
			return fmt.Errorf("failed to write chunk at offset %d: %w", offset, err)
		}
		if n < len(data) {
			return fmt.Errorf("short write at offset %d: wrote %d of %d bytes", offset, n, len(data))
		}
	} else {
		// Fallback for non-WriterAt generic io.Writer
		n, err := sw.w.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write chunk sequentially: %w", err)
		}
		if n < len(data) {
			return fmt.Errorf("short sequential write: wrote %d of %d bytes", n, len(data))
		}
	}

	sw.written[index] = true
	return nil
}

// IsComplete verifies whether all chunk indices have been written.
func (sw *StreamWriter) IsComplete() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if len(sw.written) != sw.chunkCount {
		return false
	}
	for i := 0; i < sw.chunkCount; i++ {
		if !sw.written[uint32(i)] {
			return false
		}
	}
	return true
}

// WrittenCount returns the number of chunks written so far.
func (sw *StreamWriter) WrittenCount() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.written)
}
