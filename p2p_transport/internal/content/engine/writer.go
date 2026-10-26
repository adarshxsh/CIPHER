package engine

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

// DefaultStreamingBufferSize defines the standard fixed-size buffer (64KB) allocated for streaming reassembly writes.
const DefaultStreamingBufferSize = 64 * 1024

// StreamingDiskWriter wraps a destination file handle for streaming reassembly.
// It allocates a fixed-size streaming buffer (e.g. 64KB), supports flushing and syncing,
// as well as concurrent out-of-order writes via sparse file offsets (WriteAt).
type StreamingDiskWriter struct {
	file   *os.File
	writer *bufio.Writer
}

// NewStreamingDiskWriter creates a StreamingDiskWriter wrapping the specified file handle.
func NewStreamingDiskWriter(f *os.File, bufferSize int) *StreamingDiskWriter {
	if bufferSize <= 0 {
		bufferSize = DefaultStreamingBufferSize
	}
	return &StreamingDiskWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, bufferSize),
	}
}

// Write streams sequential bytes through the fixed-size buffer.
func (s *StreamingDiskWriter) Write(p []byte) (int, error) {
	return s.writer.Write(p)
}

// WriteAt writes bytes to the destination file at a specific offset.
// Any buffered sequential writes are flushed prior to the out-of-order write.
func (s *StreamingDiskWriter) WriteAt(p []byte, off int64) (int, error) {
	if err := s.writer.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush buffer before WriteAt: %w", err)
	}
	return s.file.WriteAt(p, off)
}

// Flush flushes any pending buffered bytes to the underlying file.
func (s *StreamingDiskWriter) Flush() error {
	return s.writer.Flush()
}

// Sync flushes pending buffered bytes and calls Sync on the underlying file handle.
func (s *StreamingDiskWriter) Sync() error {
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer prior to sync: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file handle: %w", err)
	}
	return nil
}

// Close flushes, syncs, and closes the underlying file handle.
func (s *StreamingDiskWriter) Close() error {
	if err := s.writer.Flush(); err != nil {
		s.file.Close()
		return fmt.Errorf("failed to flush buffer on close: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		s.file.Close()
		return fmt.Errorf("failed to sync file on close: %w", err)
	}
	return s.file.Close()
}

// Ensure interface implementations
var (
	_ io.Writer   = (*StreamingDiskWriter)(nil)
	_ io.WriterAt = (*StreamingDiskWriter)(nil)
	_ io.Closer   = (*StreamingDiskWriter)(nil)
)
