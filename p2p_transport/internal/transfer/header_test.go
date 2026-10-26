package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestMaxFilenameLenConstant(t *testing.T) {
	if MaxFilenameLen != 255 {
		t.Fatalf("expected MaxFilenameLen to be 255, got %d", MaxFilenameLen)
	}
}

func TestHeader_WriteTo_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{"Single character", "a"},
		{"Standard filename", "testfile.txt"},
		{"Max length filename (255 bytes)", strings.Repeat("a", MaxFilenameLen)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1, 2, 3},
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if err != nil {
				t.Fatalf("unexpected error for valid filename length %d: %v", len(tc.filename), err)
			}

			if buf.Len() == 0 {
				t.Fatalf("expected written bytes, got 0")
			}
		})
	}
}

func TestHeader_WriteTo_Invalid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{"256 bytes filename", strings.Repeat("a", MaxFilenameLen+1)},
		{"500 bytes filename", strings.Repeat("b", 500)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1, 2, 3},
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if err == nil {
				t.Fatalf("expected error for filename length %d, got nil", len(tc.filename))
			}

			if !errors.Is(err, ErrFilenameTooLong) {
				t.Fatalf("expected ErrFilenameTooLong, got %v", err)
			}

			if buf.Len() != 0 {
				t.Fatalf("expected zero bytes written on validation failure, wrote %d bytes", buf.Len())
			}
		})
	}
}

func TestHeader_ReadFrom_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{"Single character", "a"},
		{"Standard filename", "hello_world.txt"},
		{"Max length filename (255 bytes)", strings.Repeat("z", MaxFilenameLen)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			orig := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 2048,
				Checksum: [32]byte{9, 8, 7},
			}

			var buf bytes.Buffer
			if err := orig.WriteTo(&buf); err != nil {
				t.Fatalf("failed to encode valid header: %v", err)
			}

			var decoded Header
			if err := decoded.ReadFrom(&buf); err != nil {
				t.Fatalf("unexpected error reading valid header: %v", err)
			}

			if decoded.Filename != tc.filename {
				t.Fatalf("expected filename %q, got %q", tc.filename, decoded.Filename)
			}
			if decoded.Version != orig.Version || decoded.Type != orig.Type || decoded.FileSize != orig.FileSize || decoded.Checksum != orig.Checksum {
				t.Fatalf("header fields mismatch after decoding")
			}
		})
	}
}

func TestHeader_ReadFrom_InvalidFilenameLength(t *testing.T) {
	testCases := []struct {
		name        string
		filenameLen uint16
	}{
		{"256 bytes length", 256},
		{"1000 bytes length", 1000},
		{"65535 bytes length (max uint16)", 65535},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Construct raw header wire bytes:
			// Version: 1 byte
			// Type: 1 byte
			// Filename Length: 2 bytes
			var buf bytes.Buffer
			buf.WriteByte(ProtocolVersion1)
			buf.WriteByte(MsgTypeFileTransfer)
			_ = binary.Write(&buf, binary.BigEndian, tc.filenameLen)

			// We explicitly do NOT append the filename payload bytes to the buffer.
			// If ReadFrom checks length before reading payload, it will fail fast with ErrFilenameTooLong.
			// If ReadFrom attempted to make([]byte, filenameLen) and io.ReadFull, it would either allocate huge memory
			// or fail with io.ErrUnexpectedEOF instead of ErrFilenameTooLong.

			var h Header
			err := h.ReadFrom(&buf)
			if err == nil {
				t.Fatalf("expected error for filename length %d, got nil", tc.filenameLen)
			}

			if !errors.Is(err, ErrFilenameTooLong) {
				t.Fatalf("expected ErrFilenameTooLong, got %v", err)
			}
		})
	}
}
