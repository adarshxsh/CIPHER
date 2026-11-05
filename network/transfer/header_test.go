package transfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_WriteAndReadValid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{name: "SingleCharFilename", filename: "a"},
		{name: "StandardFilename", filename: "test_file.txt"},
		{name: "MaxFilenameLen255", filename: strings.Repeat("x", MaxFilenameLen)},
	}

	checksum := sha256.Sum256([]byte("hello world"))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			origHeader := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 123456,
				Checksum: checksum,
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
				t.Errorf("Checksum mismatch: expected %x, got %x", origHeader.Checksum, readHeader.Checksum)
			}
		})
	}
}

func TestHeader_ReadFrom_FilenameTooLong(t *testing.T) {
	lengths := []uint16{256, 1000, 65535}

	for _, fnLen := range lengths {
		var buf bytes.Buffer
		buf.WriteByte(ProtocolVersion1)
		buf.WriteByte(MsgTypeFileTransfer)

		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], fnLen)
		buf.Write(lenBuf[:])

		var readHeader Header
		err := readHeader.ReadFrom(&buf)
		if err == nil {
			t.Fatalf("expected error for filename length %d, got nil", fnLen)
		}
		if !errors.Is(err, ErrFilenameTooLong) {
			t.Errorf("expected ErrFilenameTooLong for filename length %d, got %v", fnLen, err)
		}
	}
}

func TestHeader_ReadFrom_FilenameEmpty(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	var readHeader Header
	err := readHeader.ReadFrom(&buf)
	if err == nil {
		t.Fatal("expected error for empty filename length, got nil")
	}
	if !errors.Is(err, ErrFilenameEmpty) {
		t.Errorf("expected ErrFilenameEmpty, got %v", err)
	}
}

func TestHeader_WriteTo_FilenameTooLong(t *testing.T) {
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: strings.Repeat("a", MaxFilenameLen+1),
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := header.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for outbound filename > 255 bytes, got nil")
	}
	if !errors.Is(err, ErrFilenameTooLong) {
		t.Errorf("expected ErrFilenameTooLong, got %v", err)
	}
}

func TestHeader_WriteTo_FilenameEmpty(t *testing.T) {
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "",
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := header.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for empty outbound filename, got nil")
	}
	if !errors.Is(err, ErrFilenameEmpty) {
		t.Errorf("expected ErrFilenameEmpty, got %v", err)
	}
}

func TestHeader_ReadFrom_TruncatedStream(t *testing.T) {
	validHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "valid.txt",
		FileSize: 1024,
		Checksum: sha256.Sum256([]byte("content")),
	}

	var fullBuf bytes.Buffer
	if err := validHeader.WriteTo(&fullBuf); err != nil {
		t.Fatalf("failed to prepare valid header: %v", err)
	}

	data := fullBuf.Bytes()

	for i := 0; i < len(data); i++ {
		r := bytes.NewReader(data[:i])
		var h Header
		err := h.ReadFrom(r)
		if err == nil {
			t.Fatalf("expected error reading truncated stream of length %d / %d, got nil", i, len(data))
		}
	}
}
