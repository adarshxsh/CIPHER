package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_WriteTo_ReadFrom_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{
			name:     "single character filename",
			filename: "a",
		},
		{
			name:     "standard filename",
			filename: "document.pdf",
		},
		{
			name:     "maximum allowed length filename (255 bytes)",
			filename: strings.Repeat("x", MaxFilenameLen),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1, 2, 3, 4},
			}

			var buf bytes.Buffer
			err := original.WriteTo(&buf)
			if err != nil {
				t.Fatalf("WriteTo failed: %v", err)
			}

			var decoded Header
			err = decoded.ReadFrom(&buf)
			if err != nil {
				t.Fatalf("ReadFrom failed: %v", err)
			}

			if decoded.Version != original.Version {
				t.Errorf("Version mismatch: got %d, want %d", decoded.Version, original.Version)
			}
			if decoded.Type != original.Type {
				t.Errorf("Type mismatch: got %d, want %d", decoded.Type, original.Type)
			}
			if decoded.Filename != original.Filename {
				t.Errorf("Filename mismatch: got %q, want %q", decoded.Filename, original.Filename)
			}
			if decoded.FileSize != original.FileSize {
				t.Errorf("FileSize mismatch: got %d, want %d", decoded.FileSize, original.FileSize)
			}
			if decoded.Checksum != original.Checksum {
				t.Errorf("Checksum mismatch: got %v, want %v", decoded.Checksum, original.Checksum)
			}
		})
	}
}

func TestHeader_WriteTo_InvalidFilenameLength(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{
			name:     "empty filename",
			filename: "",
		},
		{
			name:     "oversized filename (256 bytes)",
			filename: strings.Repeat("b", MaxFilenameLen+1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 512,
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if err == nil {
				t.Fatalf("expected error for filename %q, got nil", tc.filename)
			}
			if !errors.Is(err, ErrFilenameTooLong) {
				t.Errorf("expected ErrFilenameTooLong, got %v", err)
			}
			if buf.Len() > 0 {
				t.Errorf("expected buffer to remain empty on validation failure, wrote %d bytes", buf.Len())
			}
		})
	}
}

func TestHeader_ReadFrom_InvalidFilenameLength(t *testing.T) {
	testCases := []struct {
		name        string
		filenameLen uint16
	}{
		{
			name:        "zero length filename",
			filenameLen: 0,
		},
		{
			name:        "oversized length filename (256 bytes)",
			filenameLen: 256,
		},
		{
			name:        "maximum uint16 length filename (65535 bytes)",
			filenameLen: 65535,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			// Write Version (1 byte)
			buf.WriteByte(ProtocolVersion1)
			// Write Message Type (1 byte)
			buf.WriteByte(MsgTypeFileTransfer)
			// Write Filename Length (2 bytes)
			_ = binary.Write(&buf, binary.BigEndian, tc.filenameLen)

			var h Header
			err := h.ReadFrom(&buf)
			if err == nil {
				t.Fatalf("expected error for filename length %d, got nil", tc.filenameLen)
			}
			if !errors.Is(err, ErrFilenameTooLong) {
				t.Errorf("expected ErrFilenameTooLong, got %v", err)
			}
		})
	}
}
