package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	ProtocolVersion1    byte = 1
	MsgTypeFileTransfer byte = 1
	MaxFilenameLen           = 255
)

var ErrFilenameTooLong = errors.New("filename exceeds maximum length of 255 bytes")

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
	if len(filenameBytes) > MaxFilenameLen {
		return ErrFilenameTooLong
	}

	var buf [8]byte

	// 1. Write Protocol Version
	buf[0] = h.Version
	if _, err := w.Write(buf[:1]); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// 2. Write Message Type
	buf[0] = h.Type
	if _, err := w.Write(buf[:1]); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// 3. Write Filename Length
	binary.BigEndian.PutUint16(buf[:2], uint16(len(filenameBytes)))
	if _, err := w.Write(buf[:2]); err != nil {
		return fmt.Errorf("failed to write filename length: %w", err)
	}

	// 4. Write Filename
	if _, err := w.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// 5. Write File Size
	binary.BigEndian.PutUint64(buf[:8], h.FileSize)
	if _, err := w.Write(buf[:8]); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// 6. Write Checksum
	if _, err := w.Write(h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}

// ReadFrom decodes and reads the header from the given reader.
func (h *Header) ReadFrom(r io.Reader) error {
	var buf [MaxFilenameLen]byte

	// 1. Read Protocol Version
	if _, err := io.ReadFull(r, buf[:1]); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	h.Version = buf[0]

	// 2. Read Message Type
	if _, err := io.ReadFull(r, buf[:1]); err != nil {
		return fmt.Errorf("failed to read message type: %w", err)
	}
	h.Type = buf[0]

	// 3. Read Filename Length
	if _, err := io.ReadFull(r, buf[:2]); err != nil {
		return fmt.Errorf("failed to read filename length: %w", err)
	}
	filenameLen := binary.BigEndian.Uint16(buf[:2])

	if filenameLen > MaxFilenameLen {
		return ErrFilenameTooLong
	}

	// 4. Read Filename
	filenameBytes := buf[:filenameLen]
	if _, err := io.ReadFull(r, filenameBytes); err != nil {
		return fmt.Errorf("failed to read filename: %w", err)
	}
	h.Filename = string(filenameBytes)

	// 5. Read File Size
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return fmt.Errorf("failed to read file size: %w", err)
	}
	h.FileSize = binary.BigEndian.Uint64(buf[:8])

	// 6. Read Checksum
	if _, err := io.ReadFull(r, h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	return nil
}
