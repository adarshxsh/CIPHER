package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestHeader_ValidRoundtrip(t *testing.T) {
	orig := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_file.txt",
		FileSize: 1024,
		Checksum: [32]byte{1, 2, 3, 4},
	}

	var buf bytes.Buffer
	if err := orig.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var read Header
	if err := read.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if read.Version != orig.Version {
		t.Errorf("Version mismatch: expected %d, got %d", orig.Version, read.Version)
	}
	if read.Type != orig.Type {
		t.Errorf("Type mismatch: expected %d, got %d", orig.Type, read.Type)
	}
	if read.Filename != orig.Filename {
		t.Errorf("Filename mismatch: expected %s, got %s", orig.Filename, read.Filename)
	}
	if read.FileSize != orig.FileSize {
		t.Errorf("FileSize mismatch: expected %d, got %d", orig.FileSize, read.FileSize)
	}
	if read.Checksum != orig.Checksum {
		t.Errorf("Checksum mismatch: expected %v, got %v", orig.Checksum, read.Checksum)
	}
}

func TestHeader_MaxFilenameSizeBoundary(t *testing.T) {
	filename := strings.Repeat("a", MaxFilenameSize)
	orig := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 2048,
		Checksum: [32]byte{5, 6, 7, 8},
	}

	var buf bytes.Buffer
	if err := orig.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed for max length filename: %v", err)
	}

	var read Header
	if err := read.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed for max length filename: %v", err)
	}

	if read.Filename != filename {
		t.Errorf("Filename mismatch for max length filename")
	}
}

func TestHeader_ReadFrom_OversizedFilename(t *testing.T) {
	testCases := []uint16{65535, 256, 300, 1000}

	for _, filenameLen := range testCases {
		var buf bytes.Buffer
		buf.WriteByte(ProtocolVersion1)
		buf.WriteByte(MsgTypeFileTransfer)

		var lenBytes [2]byte
		binary.BigEndian.PutUint16(lenBytes[:], filenameLen)
		buf.Write(lenBytes[:])

		var read Header
		err := read.ReadFrom(&buf)
		if err == nil {
			t.Errorf("expected error for filename length %d, got nil", filenameLen)
			continue
		}

		if !errors.Is(err, ErrFilenameLengthInvalid) {
			t.Errorf("expected ErrFilenameLengthInvalid for filename length %d, got: %v", filenameLen, err)
		}

		expectedMsg := fmt.Sprintf("filename length %d exceeds maximum allowed limit of %d bytes", filenameLen, MaxFilenameSize)
		if !strings.Contains(err.Error(), expectedMsg) {
			t.Errorf("expected error containing %q, got: %q", expectedMsg, err.Error())
		}
	}
}

func TestHeader_ReadFrom_ZeroFilenameLength(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	var lenBytes [2]byte
	binary.BigEndian.PutUint16(lenBytes[:], 0)
	buf.Write(lenBytes[:])

	var read Header
	err := read.ReadFrom(&buf)
	if err == nil {
		t.Fatal("expected error for filename length 0, got nil")
	}

	if !errors.Is(err, ErrFilenameLengthInvalid) {
		t.Errorf("expected ErrFilenameLengthInvalid for filename length 0, got: %v", err)
	}
}

func TestHeader_WriteTo_OversizedFilename(t *testing.T) {
	filename := strings.Repeat("x", MaxFilenameSize+1)
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for WriteTo with oversized filename, got nil")
	}

	if !errors.Is(err, ErrFilenameLengthInvalid) {
		t.Errorf("expected ErrFilenameLengthInvalid, got: %v", err)
	}

	expectedMsg := fmt.Sprintf("filename length %d exceeds maximum allowed limit of %d bytes", len(filename), MaxFilenameSize)
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("expected error containing %q, got: %q", expectedMsg, err.Error())
	}

	if buf.Len() > 0 {
		t.Errorf("expected zero bytes written to buffer on error, got %d bytes", buf.Len())
	}
}

func TestHeader_WriteTo_ZeroFilenameLength(t *testing.T) {
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "",
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for WriteTo with empty filename, got nil")
	}

	if !errors.Is(err, ErrFilenameLengthInvalid) {
		t.Errorf("expected ErrFilenameLengthInvalid, got: %v", err)
	}

	if buf.Len() > 0 {
		t.Errorf("expected zero bytes written to buffer on error, got %d bytes", buf.Len())
	}
}
