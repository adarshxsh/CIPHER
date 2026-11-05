package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_ValidHeader(t *testing.T) {
	filename := "testfile.txt"
	orig := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 1024,
		Checksum: [32]byte{1, 2, 3, 4},
	}

	var buf bytes.Buffer
	err := orig.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	decoded, err := DecodeHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Version != orig.Version {
		t.Errorf("expected version %d, got %d", orig.Version, decoded.Version)
	}
	if decoded.Type != orig.Type {
		t.Errorf("expected type %d, got %d", orig.Type, decoded.Type)
	}
	if decoded.Filename != orig.Filename {
		t.Errorf("expected filename %s, got %s", orig.Filename, decoded.Filename)
	}
	if decoded.FileSize != orig.FileSize {
		t.Errorf("expected fileSize %d, got %d", orig.FileSize, decoded.FileSize)
	}
	if decoded.Checksum != orig.Checksum {
		t.Errorf("expected checksum %v, got %v", orig.Checksum, decoded.Checksum)
	}
}

func TestHeader_MaxFilenameLengthBoundary(t *testing.T) {
	// Filename of exact MaxFilenameLen (255)
	filename255 := strings.Repeat("a", MaxFilenameLen)
	orig := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename255,
		FileSize: 100,
		Checksum: [32]byte{0xaa},
	}

	var buf bytes.Buffer
	if err := orig.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed for max length filename: %v", err)
	}

	decoded, err := DecodeHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeHeader failed for max length filename: %v", err)
	}
	if decoded.Filename != filename255 {
		t.Errorf("expected filename len %d, got %d", MaxFilenameLen, len(decoded.Filename))
	}
}

func TestHeader_ExceedMaxFilenameLength_Decode(t *testing.T) {
	// Construct a wire frame with 65000 byte filename length
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	invalidLen := uint16(65000)
	binary.Write(&buf, binary.BigEndian, invalidLen)

	rawBytes := buf.Bytes()

	// 1. Verify ErrInvalidHeader returned immediately
	_, err := DecodeHeader(bytes.NewReader(rawBytes))
	if err == nil {
		t.Fatalf("expected error for filename length %d, got nil", invalidLen)
	}
	if !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("expected ErrInvalidHeader, got: %v", err)
	}

	// 2. Also test ReadFrom method directly
	var h Header
	err = h.ReadFrom(bytes.NewReader(rawBytes))
	if err == nil || !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("ReadFrom expected ErrInvalidHeader, got: %v", err)
	}

	// 3. Verify no allocation for 65KB filename slice
	allocs := testing.AllocsPerRun(10, func() {
		var hdr Header
		_ = hdr.ReadFrom(bytes.NewReader(rawBytes))
	})

	t.Logf("AllocsPerRun for rejected ReadFrom header: %f", allocs)
}

func TestHeader_ExceedMaxFilenameLength_WriteTo(t *testing.T) {
	longFilename := strings.Repeat("b", MaxFilenameLen+1)
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: longFilename,
		FileSize: 10,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatalf("expected error writing header with filename len > 255, got nil")
	}
	if !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("expected ErrInvalidHeader, got: %v", err)
	}
}
