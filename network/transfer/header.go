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

var (
	ErrFilenameTooLong = errors.New("filename exceeds maximum allowed length")
	ErrInvalidFilename = errors.New("invalid filename length")
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
	filenameLen := len(h.Filename)
	if filenameLen == 0 {
		return ErrInvalidFilename
	}
	if filenameLen > MaxFilenameLen {
		return ErrFilenameTooLong
	}

	// 1. Write Protocol Version
	var versionBuf [1]byte
	versionBuf[0] = h.Version
	if _, err := w.Write(versionBuf[:]); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// 2. Write Message Type
	var typeBuf [1]byte
	typeBuf[0] = h.Type
	if _, err := w.Write(typeBuf[:]); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// 3. Write Filename Length
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(filenameLen))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed to write filename length: %w", err)
	}

	// 4. Write Filename
	filenameBytes := []byte(h.Filename)
	if _, err := w.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// 5. Write File Size
	var sizeBuf [8]byte
	binary.BigEndian.PutUint64(sizeBuf[:], h.FileSize)
	if _, err := w.Write(sizeBuf[:]); err != nil {
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
	// 1-3. Read Version (1B), Type (1B), Filename Length (2B)
	var headerBuf [4]byte
	if br, ok := r.(io.ByteReader); ok {
		for i := 0; i < 4; i++ {
			b, err := br.ReadByte()
			if err != nil {
				return fmt.Errorf("failed to read header prefix: %w", err)
			}
			headerBuf[i] = b
		}
	} else {
		if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
			return fmt.Errorf("failed to read header prefix: %w", err)
		}
	}

	h.Version = headerBuf[0]
	h.Type = headerBuf[1]
	filenameLen := binary.BigEndian.Uint16(headerBuf[2:4])

	if filenameLen == 0 {
		return ErrInvalidFilename
	}
	if filenameLen > MaxFilenameLen {
		return ErrFilenameTooLong
	}

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
