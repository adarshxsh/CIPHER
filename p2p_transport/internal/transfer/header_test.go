package transfer_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"cipher/internal/transfer"
)

func TestHeader_ReadFrom_InvalidFilenameLengths(t *testing.T) {
	invalidLengths := []uint16{0, 256, 1000, 65535}

	for _, filenameLen := range invalidLengths {
		t.Run("length_"+string(rune(filenameLen)), func(t *testing.T) {
			var buf bytes.Buffer
			// 1. Version
			buf.WriteByte(transfer.ProtocolVersion1)
			// 2. Type
			buf.WriteByte(transfer.MsgTypeFileTransfer)
			// 3. Filename Length
			binary.Write(&buf, binary.BigEndian, filenameLen)

			// We intentionally do not write filename bytes, file size, or checksum.
			// If ReadFrom attempted to read filename bytes for an invalid length,
			// it would fail with EOF or truncated error instead of ErrInvalidFilenameLength.

			var h transfer.Header
			err := h.ReadFrom(&buf)
			if err == nil {
				t.Fatalf("expected error for filename length %d, got nil", filenameLen)
			}
			if !errors.Is(err, transfer.ErrInvalidFilenameLength) {
				t.Errorf("expected ErrInvalidFilenameLength, got: %v", err)
			}
		})
	}
}

func TestHeader_WriteTo_InvalidFilenameLengths(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{
			name:     "empty_filename",
			filename: "",
		},
		{
			name:     "oversized_filename_256",
			filename: strings.Repeat("a", 256),
		},
		{
			name:     "oversized_filename_500",
			filename: strings.Repeat("x", 500),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &transfer.Header{
				Version:  transfer.ProtocolVersion1,
				Type:     transfer.MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1, 2, 3},
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if err == nil {
				t.Fatalf("expected error for filename length %d, got nil", len(tc.filename))
			}
			if !errors.Is(err, transfer.ErrInvalidFilenameLength) {
				t.Errorf("expected ErrInvalidFilenameLength, got: %v", err)
			}
			if buf.Len() != 0 {
				t.Errorf("expected 0 bytes written to writer on validation failure, got %d", buf.Len())
			}
		})
	}
}

func TestHeader_ValidRoundTrip(t *testing.T) {
	validFilenames := []string{
		"a",
		"test.txt",
		"a_very_long_valid_filename_with_extensions.tar.gz",
		strings.Repeat("z", 255), // Exact max boundary
	}

	for _, fn := range validFilenames {
		t.Run("length_"+string(rune(len(fn))), func(t *testing.T) {
			orig := &transfer.Header{
				Version:  transfer.ProtocolVersion1,
				Type:     transfer.MsgTypeFileTransfer,
				Filename: fn,
				FileSize: 987654321,
				Checksum: [32]byte{0xab, 0xcd, 0xef, 0x12},
			}

			var buf bytes.Buffer
			if err := orig.WriteTo(&buf); err != nil {
				t.Fatalf("WriteTo failed for valid filename of length %d: %v", len(fn), err)
			}

			var decoded transfer.Header
			if err := decoded.ReadFrom(&buf); err != nil {
				t.Fatalf("ReadFrom failed for valid filename of length %d: %v", len(fn), err)
			}

			if decoded.Version != orig.Version {
				t.Errorf("Version mismatch: expected %d, got %d", orig.Version, decoded.Version)
			}
			if decoded.Type != orig.Type {
				t.Errorf("Type mismatch: expected %d, got %d", orig.Type, decoded.Type)
			}
			if decoded.Filename != orig.Filename {
				t.Errorf("Filename mismatch: expected %s, got %s", orig.Filename, decoded.Filename)
			}
			if decoded.FileSize != orig.FileSize {
				t.Errorf("FileSize mismatch: expected %d, got %d", orig.FileSize, decoded.FileSize)
			}
			if decoded.Checksum != orig.Checksum {
				t.Errorf("Checksum mismatch: expected %x, got %x", orig.Checksum, decoded.Checksum)
			}
		})
	}
}
