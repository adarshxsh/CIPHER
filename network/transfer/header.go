package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	ProtocolVersion1    byte = 1
	MsgTypeFileTransfer byte = 1
	MaxFilenameLen           = 255
)

var (
	ErrFilenameTooLong = errors.New("filename exceeds maximum allowed length")
	ErrInvalidFilename = errors.New("filename length must be greater than zero")
)

// Header represents the binary metadata sent before the file contents.
// Wire Format (Big Endian):
// [1 byte] Protocol Version
// [1 byte] Message Type
// [2 bytes] Filename Length (N)
// [N bytes] Filename
// [8 bytes] File Size
// [32 bytes] SHA-256 Checksum
type Header struct {
	Version  byte
	Type     byte
	Filename string
	FileSize uint64
	Checksum [32]byte
}

// WriteTo encodes and writes the header to the given writer.
func (h *Header) WriteTo(w io.Writer) error {
	filenameBytes := []byte(h.Filename)
	filenameLen := len(filenameBytes)
	if filenameLen == 0 {
		return ErrInvalidFilename
	}
	if filenameLen > MaxFilenameLen {
		return ErrFilenameTooLong
	}

	// 1. Write Protocol Version (1 byte), Message Type (1 byte), Filename Length (2 bytes)
	var prefix [4]byte
	prefix[0] = h.Version
	prefix[1] = h.Type
	binary.BigEndian.PutUint16(prefix[2:4], uint16(filenameLen))
	if _, err := w.Write(prefix[:]); err != nil {
		return fmt.Errorf("failed to write header prefix: %w", err)
	}

	// 2. Write Filename
	if _, err := w.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// 3. Write File Size
	var sizeBuf [8]byte
	binary.BigEndian.PutUint64(sizeBuf[:], h.FileSize)
	if _, err := w.Write(sizeBuf[:]); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// 4. Write Checksum
	if _, err := w.Write(h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}

var prefixPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4)
		return &b
	},
}

// ReadFrom decodes and reads the header from the given reader.
func (h *Header) ReadFrom(r io.Reader) error {
	// 1-3. Read Protocol Version (1 byte), Message Type (1 byte), Filename Length (2 bytes)
	bufPtr := prefixPool.Get().(*[]byte)
	prefix := *bufPtr

	if _, err := io.ReadFull(r, prefix); err != nil {
		prefixPool.Put(bufPtr)
		return fmt.Errorf("failed to read header prefix: %w", err)
	}

	h.Version = prefix[0]
	h.Type = prefix[1]
	filenameLen := binary.BigEndian.Uint16(prefix[2:4])

	if filenameLen == 0 {
		prefixPool.Put(bufPtr)
		return ErrInvalidFilename
	}
	if filenameLen > MaxFilenameLen {
		prefixPool.Put(bufPtr)
		return ErrFilenameTooLong
	}
	prefixPool.Put(bufPtr)

	// 4. Read Filename
	filenameBytes := make([]byte, filenameLen)
	if _, err := io.ReadFull(r, filenameBytes); err != nil {
		return fmt.Errorf("failed to read filename: %w", err)
	}
	h.Filename = string(filenameBytes)

	// 5. Read File Size
	var sizeBuf [8]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return fmt.Errorf("failed to read file size: %w", err)
	}
	h.FileSize = binary.BigEndian.Uint64(sizeBuf[:])

	// 6. Read Checksum
	if _, err := io.ReadFull(r, h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	return nil
}
