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

	readHeader, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
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

func TestHeader_ReadWrite_MaxHeaderStringLength(t *testing.T) {
	// Header string of exact MaxHeaderStringLength (1024 bytes)
	maxFilename := strings.Repeat("a", MaxHeaderStringLength)
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: maxFilename,
		FileSize: 2048,
		Checksum: [32]byte{0xaa, 0xbb},
	}

	var buf bytes.Buffer
	if err := origHeader.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo with MaxHeaderStringLength failed: %v", err)
	}

	readHeader, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader with MaxHeaderStringLength failed: %v", err)
	}

	if readHeader.Filename != maxFilename {
		t.Fatalf("Filename mismatch: expected length %d, got length %d", len(maxFilename), len(readHeader.Filename))
	}
}

func TestHeader_WriteTo_OversizedFilename(t *testing.T) {
	oversizedFilename := strings.Repeat("x", MaxHeaderStringLength+1) // 1025 bytes
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: oversizedFilename,
		FileSize: 512,
	}

	var buf bytes.Buffer
	err := header.WriteTo(&buf)
	if err == nil {
		t.Fatalf("expected error when writing header with string length > MaxHeaderStringLength, got nil")
	}

	if !errors.Is(err, ErrHeaderStringTooLong) {
		t.Fatalf("expected ErrHeaderStringTooLong, got %v", err)
	}

	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written on error, wrote %d bytes", buf.Len())
	}
}

func TestHeader_ReadFrom_OversizedFilenameLen(t *testing.T) {
	// Construct raw byte stream with filenameLen > MaxHeaderStringLength (1025 bytes)
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	oversizedLen := uint16(1025)
	_ = binary.Write(&buf, binary.BigEndian, oversizedLen)

	var header Header
	err := header.ReadFrom(&buf)
	if err == nil {
		t.Fatalf("expected error when reading frame with oversized string length, got nil")
	}

	if !errors.Is(err, ErrHeaderStringTooLong) {
		t.Fatalf("expected ErrHeaderStringTooLong, got %v", err)
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
		t.Fatalf("expected error when reading frame with uint16 max string length, got nil")
	}

	if !errors.Is(err, ErrHeaderStringTooLong) {
		t.Fatalf("expected ErrHeaderStringTooLong, got %v", err)
	}
}

func TestReadHeader_Oversized(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	oversizedLen := uint16(1025)
	_ = binary.Write(&buf, binary.BigEndian, oversizedLen)

	readHeader, err := ReadHeader(&buf)
	if err == nil {
		t.Fatalf("expected error from ReadHeader with string length > 1024, got header: %v", readHeader)
	}

	if !errors.Is(err, ErrHeaderStringTooLong) {
		t.Fatalf("expected ErrHeaderStringTooLong, got %v", err)
	}
}
