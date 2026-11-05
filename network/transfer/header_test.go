package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeader_RoundTrip_Valid(t *testing.T) {
	testCases := []struct {
		name     string
		filename string
	}{
		{
			name:     "short filename",
			filename: "test.txt",
		},
		{
			name:     "255-byte filename",
			filename: strings.Repeat("a", 255),
		},
		{
			name:     "512-byte filename (max limit)",
			filename: strings.Repeat("b", MaxFilenameLength),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			origHeader := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.filename,
				FileSize: 1048576,
				Checksum: [32]byte{1, 2, 3, 4, 5},
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
				t.Errorf("Version mismatch: got %d, want %d", readHeader.Version, origHeader.Version)
			}
			if readHeader.Type != origHeader.Type {
				t.Errorf("Type mismatch: got %d, want %d", readHeader.Type, origHeader.Type)
			}
			if readHeader.Filename != origHeader.Filename {
				t.Errorf("Filename mismatch: length got %d, want %d", len(readHeader.Filename), len(origHeader.Filename))
			}
			if readHeader.FileSize != origHeader.FileSize {
				t.Errorf("FileSize mismatch: got %d, want %d", readHeader.FileSize, origHeader.FileSize)
			}
			if readHeader.Checksum != origHeader.Checksum {
				t.Errorf("Checksum mismatch")
			}
		})
	}
}

func TestHeader_WriteTo_OversizedFilename(t *testing.T) {
	oversizedFilename := strings.Repeat("x", MaxFilenameLength+1)
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: oversizedFilename,
		FileSize: 100,
	}

	var buf bytes.Buffer
	err := h.WriteTo(&buf)
	if err == nil {
		t.Fatal("expected error when writing oversized filename, got nil")
	}
	if !errors.Is(err, ErrFilenameTooLong) {
		t.Fatalf("expected ErrFilenameTooLong, got %v", err)
	}
}

func TestHeader_ReadFrom_OversizedFilenameHeader(t *testing.T) {
	testLengths := []uint16{513, 1000, 32768, 65535}

	for _, lenVal := range testLengths {
		var buf bytes.Buffer
		// Write Protocol Version (1 byte)
		buf.WriteByte(ProtocolVersion1)
		// Write Message Type (1 byte)
		buf.WriteByte(MsgTypeFileTransfer)
		// Write Filename Length (> 512) (2 bytes)
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, lenVal)
		buf.Write(lenBytes)

		var h Header
		err := h.ReadFrom(&buf)
		if err == nil {
			t.Fatalf("len %d: expected error reading header with filename length %d, got nil", lenVal, lenVal)
		}
		if !errors.Is(err, ErrFilenameTooLong) {
			t.Fatalf("len %d: expected ErrFilenameTooLong, got %v", lenVal, err)
		}
	}
}

func TestHeader_ReadFrom_TruncatedStreams(t *testing.T) {
	// Truncated version / type / len
	fullData := []byte{1, 1, 0, 8, 't', 'e', 's', 't', '.', 't', 'x', 't'}

	for i := 0; i < len(fullData)-1; i++ {
		r := bytes.NewReader(fullData[:i])
		var h Header
		err := h.ReadFrom(r)
		if err == nil {
			t.Fatalf("expected error reading truncated stream at length %d, got nil", i)
		}
	}
}
