package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_WriteTo_ReadFrom_Success(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{
			name:     "standard filename",
			filename: "testfile.txt",
		},
		{
			name:     "max length filename (255 bytes)",
			filename: strings.Repeat("a", 255),
		},
		{
			name:     "empty filename",
			filename: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
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

func TestHeader_WriteTo_FilenameTooLong(t *testing.T) {
	h := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: strings.Repeat("x", 256),
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for filename longer than 255 bytes, got nil")
	}
	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no bytes written to writer on error, wrote %d bytes", buf.Len())
	}
}

func TestHeader_ReadFrom_FilenameTooLong(t *testing.T) {
	// Craft a header frame with filename length set to 256 (or 65535)
	var buf bytes.Buffer
	buf.WriteByte(ProtocolVersion1)
	buf.WriteByte(MsgTypeFileTransfer)

	// Write length 256
	_ = binary.Write(&buf, binary.BigEndian, uint16(256))

	// We intentionally do not write 256 bytes of filename data to ensure
	// ReadFrom returns before attempting to read filename bytes.

	var h Header
	err := h.ReadFrom(&buf)
	if err == nil {
		t.Fatal("expected error for filename length 256, got nil")
	}
	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
}

func BenchmarkHeader_ReadFrom(b *testing.B) {
	h := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "sample_benchmark_filename.ext",
		FileSize: 4096,
		Checksum: [32]byte{0xab},
	}
	var buf bytes.Buffer
	if err := h.WriteTo(&buf); err != nil {
		b.Fatalf("failed to write header: %v", err)
	}
	encoded := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(encoded)
		var decoded Header
		if err := decoded.ReadFrom(r); err != nil {
			b.Fatalf("ReadFrom failed: %v", err)
		}
	}
}
