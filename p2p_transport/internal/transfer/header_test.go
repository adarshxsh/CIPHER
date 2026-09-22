package transfer

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestHeader_RoundTrip(t *testing.T) {
	orig := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "testfile.txt",
		FileSize: 1024,
		Checksum: [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
	}

	var buf bytes.Buffer
	if err := orig.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var decoded Header
	if err := decoded.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if decoded.Version != orig.Version {
		t.Errorf("Version mismatch: expected %d, got %d", orig.Version, decoded.Version)
	}
	if decoded.Type != orig.Type {
		t.Errorf("Type mismatch: expected %d, got %d", orig.Type, decoded.Type)
	}
	if decoded.Filename != orig.Filename {
		t.Errorf("Filename mismatch: expected %q, got %q", orig.Filename, decoded.Filename)
	}
	if decoded.FileSize != orig.FileSize {
		t.Errorf("FileSize mismatch: expected %d, got %d", orig.FileSize, decoded.FileSize)
	}
	if decoded.Checksum != orig.Checksum {
		t.Errorf("Checksum mismatch: expected %v, got %v", orig.Checksum, decoded.Checksum)
	}
}

func TestHeader_MaxFilenameSize_Boundary(t *testing.T) {
	maxName := strings.Repeat("a", int(MaxFilenameSize)) // 255 bytes
	orig := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: maxName,
		FileSize: 500,
		Checksum: [32]byte{0xAA},
	}

	var buf bytes.Buffer
	if err := orig.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed for max allowed filename length (%d): %v", MaxFilenameSize, err)
	}

	var decoded Header
	if err := decoded.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed for max allowed filename length (%d): %v", MaxFilenameSize, err)
	}

	if decoded.Filename != maxName {
		t.Errorf("Filename mismatch for boundary test: length got %d, expected %d", len(decoded.Filename), len(maxName))
	}
}

func TestHeader_ExceedsMaxFilenameSize_WriteTo(t *testing.T) {
	tooLongName := strings.Repeat("b", int(MaxFilenameSize)+1) // 256 bytes
	hdr := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: tooLongName,
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := hdr.WriteTo(&buf)
	if err == nil {
		t.Fatalf("expected error when WriteTo filename length (%d) exceeds MaxFilenameSize (%d), got nil", len(tooLongName), MaxFilenameSize)
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHeader_ExceedsMaxFilenameSize_ReadFrom(t *testing.T) {
	// Construct malformed header payload with filename length exceeding MaxFilenameSize (e.g. 256 or 65535)
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	// Filename length = 256 (0x0100)
	filenameLen := uint16(MaxFilenameSize + 1)
	binary.Write(&buf, binary.BigEndian, filenameLen)

	// Write 256 bytes of dummy filename
	buf.Write(bytes.Repeat([]byte{'x'}, int(filenameLen)))
	binary.Write(&buf, binary.BigEndian, uint64(1024))
	buf.Write(make([]byte, 32))

	var decoded Header
	err := decoded.ReadFrom(&buf)
	if err == nil {
		t.Fatalf("expected ReadFrom to fail for filenameLen %d > MaxFilenameSize %d", filenameLen, MaxFilenameSize)
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowed size") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHeader_ReadFrom_Truncated(t *testing.T) {
	// Truncated version
	var buf bytes.Buffer
	var decoded Header
	if err := decoded.ReadFrom(&buf); err == nil {
		t.Error("expected error on empty reader, got nil")
	}

	// Truncated filename length
	buf.Reset()
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)
	if err := decoded.ReadFrom(&buf); err == nil {
		t.Error("expected error on truncated filename length, got nil")
	}

	// Truncated filename content
	buf.Reset()
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)
	binary.Write(&buf, binary.BigEndian, uint16(10))
	buf.WriteString("short") // only 5 bytes written instead of 10
	if err := decoded.ReadFrom(&buf); err == nil {
		t.Error("expected error on truncated filename content, got nil")
	}
}
