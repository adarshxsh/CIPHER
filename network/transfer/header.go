package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	ProtocolVersion1      byte = 1
	MsgTypeFileTransfer   byte = 1
	MaxHeaderStringLength      = 1024
)

var (
	ErrHeaderStringTooLong = errors.New("header string length exceeds maximum limit")
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
	if len(filenameBytes) > MaxHeaderStringLength {
		return fmt.Errorf("%w: length %d exceeds maximum limit of %d", ErrHeaderStringTooLong, len(filenameBytes), MaxHeaderStringLength)
	}

	// 1. Write Protocol Version
	if err := binary.Write(w, binary.BigEndian, h.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// 2. Write Message Type
	if err := binary.Write(w, binary.BigEndian, h.Type); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// 3. Write Filename Length
	filenameLen := uint16(len(filenameBytes))
	if err := binary.Write(w, binary.BigEndian, filenameLen); err != nil {
		return fmt.Errorf("failed to write filename length: %w", err)
	}

	// 4. Write Filename
	if _, err := w.Write(filenameBytes); err != nil {
		return fmt.Errorf("failed to write filename: %w", err)
	}

	// 5. Write File Size
	if err := binary.Write(w, binary.BigEndian, h.FileSize); err != nil {
		return fmt.Errorf("failed to write file size: %w", err)
	}

	// 6. Write Checksum
	if _, err := w.Write(h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}

// ReadHeader decodes and reads a Header from the given reader.
func ReadHeader(r io.Reader) (*Header, error) {
	var h Header
	if err := h.ReadFrom(r); err != nil {
		return nil, err
	}
	return &h, nil
}

// ReadFrom decodes and reads the header from the given reader.
func (h *Header) ReadFrom(r io.Reader) error {
	// 1. Read Protocol Version
	if err := binary.Read(r, binary.BigEndian, &h.Version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	// 2. Read Message Type
	if err := binary.Read(r, binary.BigEndian, &h.Type); err != nil {
		return fmt.Errorf("failed to read message type: %w", err)
	}

	// 3. Read Filename Length
	var filenameLen uint16
	if err := binary.Read(r, binary.BigEndian, &filenameLen); err != nil {
		return fmt.Errorf("failed to read filename length: %w", err)
	}

	if filenameLen > MaxHeaderStringLength {
		return fmt.Errorf("%w: length %d exceeds maximum limit of %d", ErrHeaderStringTooLong, filenameLen, MaxHeaderStringLength)
	}

	// 4. Read Filename
	filenameBytes := make([]byte, filenameLen)
	if _, err := io.ReadFull(r, filenameBytes); err != nil {
		return fmt.Errorf("failed to read filename: %w", err)
	}
	h.Filename = string(filenameBytes)

	// 5. Read File Size
	if err := binary.Read(r, binary.BigEndian, &h.FileSize); err != nil {
		return fmt.Errorf("failed to read file size: %w", err)
	}

	// 6. Read Checksum
	if _, err := io.ReadFull(r, h.Checksum[:]); err != nil {
		return fmt.Errorf("failed to read checksum: %w", err)
	}

	return nil
}
