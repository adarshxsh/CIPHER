package transfer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestHeader_ReadFrom_Valid(t *testing.T) {
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test.txt",
		FileSize: 1024,
		Checksum: [32]byte{0x01, 0x02, 0x03},
	}

	var buf bytes.Buffer
	if err := origHeader.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	readHeader := &Header{}
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readHeader.Filename != origHeader.Filename {
		t.Errorf("filename mismatch: got %s, want %s", readHeader.Filename, origHeader.Filename)
	}
	if readHeader.FileSize != origHeader.FileSize {
		t.Errorf("file size mismatch: got %d, want %d", readHeader.FileSize, origHeader.FileSize)
	}
}

func TestHeader_ReadFrom_OversizedFilename(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)
	// Filename length 1000 (> MaxFilenameSize 255)
	_ = binary.Write(&buf, binary.BigEndian, uint16(1000))

	header := &Header{}
	err := header.ReadFrom(&buf)
	if err == nil {
		t.Fatalf("Expected ReadFrom to fail on oversized filename length before allocation, got nil error")
	}
}
