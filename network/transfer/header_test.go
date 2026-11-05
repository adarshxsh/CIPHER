package transfer

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestHeader_WriteTo_ReadFrom_Valid(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{
			name:     "standard filename",
			filename: "document.pdf",
		},
		{
			name:     "single character filename",
			filename: "a",
		},
		{
			name:     "maximum allowed filename length (255 bytes)",
			filename: strings.Repeat("x", MaxFilenameLength),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
				FileSize: 1024 * 1024,
				Checksum: [32]byte{1, 2, 3, 4, 5},
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
		})
	}
}

func TestHeader_ReadFrom_InvalidFilenameLength(t *testing.T) {
	tests := []struct {
		name        string
		filenameLen uint16
	}{
		{
			name:        "zero filename length",
			filenameLen: 0,
		},
		{
			name:        "oversized filename length (256 bytes)",
			filenameLen: MaxFilenameLength + 1,
		},
		{
			name:        "oversized filename length (65535 bytes)",
			filenameLen: 65535,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			// Write Version (1 byte)
			_ = binary.Write(&buf, binary.BigEndian, ProtocolVersion1)
			// Write Type (1 byte)
			_ = binary.Write(&buf, binary.BigEndian, MsgTypeFileTransfer)
			// Write Filename Length (2 bytes)
			_ = binary.Write(&buf, binary.BigEndian, tt.filenameLen)

			var header Header
			err := header.ReadFrom(&buf)
			if err == nil {
				t.Fatalf("ReadFrom expected error for filename length %d, got nil", tt.filenameLen)
			}

			expectedErrSubstr := "invalid filename length"
			if !strings.Contains(err.Error(), expectedErrSubstr) {
				t.Errorf("expected error containing %q, got %q", expectedErrSubstr, err.Error())
			}
		})
	}
}

func TestHeader_WriteTo_InvalidFilenameLength(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{
			name:     "empty filename",
			filename: "",
		},
		{
			name:     "oversized filename (256 bytes)",
			filename: strings.Repeat("b", MaxFilenameLength+1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
				FileSize: 100,
				Checksum: [32]byte{},
			}

			var buf bytes.Buffer
			err := header.WriteTo(&buf)
			if err == nil {
				t.Fatalf("WriteTo expected error for filename length %d, got nil", len(tt.filename))
			}

			expectedErrSubstr := "invalid filename length"
			if !strings.Contains(err.Error(), expectedErrSubstr) {
				t.Errorf("expected error containing %q, got %q", expectedErrSubstr, err.Error())
			}
		})
	}
}
