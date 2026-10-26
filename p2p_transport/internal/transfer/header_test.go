package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_ReadWrite_Valid(t *testing.T) {
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_document.pdf",
		FileSize: 1024,
		Checksum: [32]byte{1, 2, 3, 4, 5},
	}

	var buf bytes.Buffer
	if err := origHeader.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readHeader.Version != origHeader.Version {
		t.Errorf("Version mismatch: expected %d, got %d", origHeader.Version, readHeader.Version)
	}
	if readHeader.Type != origHeader.Type {
		t.Errorf("Type mismatch: expected %d, got %d", origHeader.Type, readHeader.Type)
	}
	if readHeader.Filename != origHeader.Filename {
		t.Errorf("Filename mismatch: expected %q, got %q", origHeader.Filename, readHeader.Filename)
	}
	if readHeader.FileSize != origHeader.FileSize {
		t.Errorf("FileSize mismatch: expected %d, got %d", origHeader.FileSize, readHeader.FileSize)
	}
	if readHeader.Checksum != origHeader.Checksum {
		t.Errorf("Checksum mismatch: expected %v, got %v", origHeader.Checksum, readHeader.Checksum)
	}
}

func TestHeader_ReadWrite_MaxFilenameSize(t *testing.T) {
	// Filename of exact MaxFilenameSize (255 bytes)
	maxFilename := strings.Repeat("a", MaxFilenameSize)
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: maxFilename,
		FileSize: 2048,
		Checksum: [32]byte{0xaa, 0xbb},
	}

	var buf bytes.Buffer
	if err := origHeader.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo with MaxFilenameSize failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom with MaxFilenameSize failed: %v", err)
	}

	if readHeader.Filename != maxFilename {
		t.Fatalf("Filename mismatch: expected length %d, got length %d", len(maxFilename), len(readHeader.Filename))
	}
}

func TestHeader_WriteTo_OversizedFilename(t *testing.T) {
	oversizedFilename := strings.Repeat("x", MaxFilenameSize+1) // 256 bytes
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: oversizedFilename,
		FileSize: 512,
	}

	var buf bytes.Buffer
	err := header.WriteTo(&buf)
	if err == nil {
		t.Fatalf("expected error when writing oversized filename, got nil")
	}

	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}

	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written on error, wrote %d bytes", buf.Len())
	}
}

func TestHeader_ReadFrom_OversizedFilenameLen(t *testing.T) {
	// Construct raw byte stream with filenameLen > MaxFilenameSize
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	// Write filenameLen = 256 (or 65535)
	oversizedLen := uint16(256)
	_ = binary.Write(&buf, binary.BigEndian, oversizedLen)

	var header Header
	err := header.ReadFrom(&buf)
	if err == nil {
		t.Fatalf("expected error when reading frame with oversized filename length, got nil")
	}

	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
}

func TestHeader_ReadFrom_MaxUint16FilenameLen(t *testing.T) {
	// Construct raw byte stream with filenameLen = 65535
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	_ = binary.Write(&buf, binary.BigEndian, uint16(65535))

	var header Header
	err := header.ReadFrom(&buf)
	if err == nil {
		t.Fatalf("expected error when reading frame with uint16 max filename length, got nil")
	}

	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
}
