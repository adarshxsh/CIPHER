package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_WriteTo_Valid(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{
			name:     "standard filename",
			filename: "document.pdf",
		},
		{
			name:     "single byte filename",
			filename: "a",
		},
		{
			name:     "max length filename 255 bytes",
			filename: strings.Repeat("x", MaxFilenameLen),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1, 2, 3},
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Read back header to verify roundtrip
			var readHeader Header
			err = readHeader.ReadFrom(&buf)
			if err != nil {
				t.Fatalf("unexpected error reading written header: %v", err)
			}

			if readHeader.Filename != tc.filename {
				t.Errorf("expected filename %q, got %q", tc.filename, readHeader.Filename)
			}
			if readHeader.FileSize != h.FileSize {
				t.Errorf("expected fileSize %d, got %d", h.FileSize, readHeader.FileSize)
			}
			if readHeader.Checksum != h.Checksum {
				t.Errorf("expected checksum %v, got %v", h.Checksum, readHeader.Checksum)
			}
		})
	}
}

func TestHeader_WriteTo_EmptyFilename(t *testing.T) {
	h := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "",
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for empty filename, got nil")
	}

	if !errors.Is(err, ErrFilenameEmpty) {
		t.Errorf("expected ErrFilenameEmpty, got %v", err)
	}

	if buf.Len() > 0 {
		t.Errorf("expected no bytes written to stream on error, wrote %d bytes", buf.Len())
	}
}

func TestHeader_WriteTo_FilenameTooLong(t *testing.T) {
	h := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: strings.Repeat("a", MaxFilenameLen+1),
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error for filename exceeding MaxFilenameLen, got nil")
	}

	if !errors.Is(err, ErrFilenameTooLong) {
		t.Errorf("expected ErrFilenameTooLong, got %v", err)
	}

	if buf.Len() > 0 {
		t.Errorf("expected no bytes written to stream on error, wrote %d bytes", buf.Len())
	}
}

func TestHeader_ReadFrom_ZeroFilenameLen(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, ProtocolVersion1)
	_ = binary.Write(&buf, binary.BigEndian, MsgTypeFileTransfer)
	_ = binary.Write(&buf, binary.BigEndian, uint16(0)) // Filename length = 0

	var h Header
	err := h.ReadFrom(&buf)
	if err == nil {
		t.Fatal("expected error for zero filename length, got nil")
	}

	if !errors.Is(err, ErrFilenameEmpty) {
		t.Errorf("expected ErrFilenameEmpty, got %v", err)
	}
}

func TestHeader_ReadFrom_FilenameLenTooLong(t *testing.T) {
	invalidLengths := []uint16{256, 1000, 65535}

	for _, lenVal := range invalidLengths {
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.BigEndian, ProtocolVersion1)
		_ = binary.Write(&buf, binary.BigEndian, MsgTypeFileTransfer)
		_ = binary.Write(&buf, binary.BigEndian, lenVal) // Exceeds MaxFilenameLen (255)

		var h Header
		err := h.ReadFrom(&buf)
		if err == nil {
			t.Fatalf("expected error for filename length %d, got nil", lenVal)
		}

		if !errors.Is(err, ErrFilenameTooLong) {
			t.Errorf("expected ErrFilenameTooLong for length %d, got %v", lenVal, err)
		}
	}
}

func TestHeader_ReadFrom_NoAllocationOnInvalidLen(t *testing.T) {
	// Frame header with version=1, type=1, filename length=65535 (2 bytes)
	// Notice we DO NOT provide 65535 payload bytes in the stream buffer.
	// If ReadFrom did not validate filenameLen before allocating make([]byte, filenameLen),
	// it would allocate 65535 bytes before returning an io.ErrUnexpectedEOF.
	// With validation prior to allocation, it returns ErrFilenameTooLong immediately.
	rawHeader := []byte{ProtocolVersion1, MsgTypeFileTransfer, 0xFF, 0xFF}

	var h Header
	err := h.ReadFrom(bytes.NewReader(rawHeader))
	if err == nil {
		t.Fatal("expected error for malformed header length, got nil")
	}

	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
}
