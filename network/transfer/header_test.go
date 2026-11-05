package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestHeader_WriteTo_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{name: "1 byte filename", filename: "a"},
		{name: "100 byte filename", filename: strings.Repeat("b", 100)},
		{name: "255 byte filename (MaxFilenameLen)", filename: strings.Repeat("c", 255)},
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
				t.Fatalf("expected WriteTo to succeed for filename len %d, got: %v", len(tc.filename), err)
			}

			expectedLen := 1 + 1 + 2 + len(tc.filename) + 8 + 32
			if buf.Len() != expectedLen {
				t.Fatalf("expected written buffer size %d, got %d", expectedLen, buf.Len())
			}
		})
	}
}

func TestHeader_WriteTo_Invalid(t *testing.T) {
	testCases := []struct {
		name        string
		filename    string
		expectedErr error
	}{
		{
			name:        "empty filename",
			filename:    "",
			expectedErr: ErrInvalidFilename,
		},
		{
			name:        "oversized filename (256 bytes)",
			filename:    strings.Repeat("a", 256),
			expectedErr: ErrFilenameTooLong,
		},
		{
			name:        "oversized filename (1000 bytes)",
			filename:    strings.Repeat("x", 1000),
			expectedErr: ErrFilenameTooLong,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1024,
				Checksum: [32]byte{1},
			}

			var buf bytes.Buffer
			err := h.WriteTo(&buf)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}

			if buf.Len() != 0 {
				t.Fatalf("expected 0 bytes written to buffer on invalid filename, got %d", buf.Len())
			}
		})
	}
}

func TestHeader_ReadFrom_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{name: "1 byte filename", filename: "z"},
		{name: "100 byte filename", filename: strings.Repeat("m", 100)},
		{name: "255 byte filename (MaxFilenameLen)", filename: strings.Repeat("k", 255)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			orig := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 2048,
				Checksum: [32]byte{0xaa, 0xbb},
			}

			var buf bytes.Buffer
			if err := orig.WriteTo(&buf); err != nil {
				t.Fatalf("failed to write original header: %v", err)
			}

			var decoded Header
			if err := decoded.ReadFrom(&buf); err != nil {
				t.Fatalf("expected ReadFrom to succeed, got: %v", err)
			}

			if decoded.Version != orig.Version || decoded.Type != orig.Type ||
				decoded.Filename != orig.Filename || decoded.FileSize != orig.FileSize ||
				decoded.Checksum != orig.Checksum {
				t.Fatalf("decoded header %+v does not match original %+v", decoded, orig)
			}
		})
	}
}

func TestHeader_ReadFrom_Invalid(t *testing.T) {
	testCases := []struct {
		name        string
		filenameLen uint16
		expectedErr error
	}{
		{
			name:        "zero length filename",
			filenameLen: 0,
			expectedErr: ErrInvalidFilename,
		},
		{
			name:        "oversized filename (256 bytes)",
			filenameLen: 256,
			expectedErr: ErrFilenameTooLong,
		},
		{
			name:        "oversized filename (65535 bytes)",
			filenameLen: 65535,
			expectedErr: ErrFilenameTooLong,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteByte(ProtocolVersion1)
			buf.WriteByte(MsgTypeFileTransfer)
			binary.Write(&buf, binary.BigEndian, tc.filenameLen)

			var decoded Header
			err := decoded.ReadFrom(&buf)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestHeader_ReadFrom_NoAllocationOnOversized(t *testing.T) {
	// Construct header with filename length 65535
	var rawHeader []byte
	rawHeader = append(rawHeader, ProtocolVersion1)
	rawHeader = append(rawHeader, MsgTypeFileTransfer)
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, 65535)
	rawHeader = append(rawHeader, lenBuf...)

	r := bytes.NewReader(rawHeader)
	var reader io.Reader = r
	var h Header

	allocs := testing.AllocsPerRun(10, func() {
		r.Reset(rawHeader)
		_ = h.ReadFrom(reader)
	})

	// Reading the 4-byte prefix over io.Reader interface uses at most 1 slice allocation for the header prefix,
	// and strictly 0 payload allocations for the 65535-byte filename.
	if allocs > 1 {
		t.Fatalf("expected at most 1 heap allocation when reading oversized filename header prefix, got %f", allocs)
	}
}

func TestHeader_RoundTrip(t *testing.T) {
	h1 := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_file.txt",
		FileSize: 9999,
		Checksum: [32]byte{1, 2, 3, 4, 5},
	}

	var buf bytes.Buffer
	if err := h1.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var h2 Header
	if err := h2.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if h1.Version != h2.Version || h1.Type != h2.Type ||
		h1.Filename != h2.Filename || h1.FileSize != h2.FileSize ||
		h1.Checksum != h2.Checksum {
		t.Fatalf("Roundtrip mismatch. Expected %+v, got %+v", h1, h2)
	}
}
