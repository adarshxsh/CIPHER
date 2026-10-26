package transfer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeaderReadFrom_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name        string
		filenameLen uint16
		expectedErr error
	}{
		{
			name:        "Filename length 0",
			filenameLen: 0,
			expectedErr: ErrInvalidFilename,
		},
		{
			name:        "Filename length 1",
			filenameLen: 1,
			expectedErr: nil,
		},
		{
			name:        "Filename length 255 (MaxFilenameLen)",
			filenameLen: 255,
			expectedErr: nil,
		},
		{
			name:        "Filename length 256 (MaxFilenameLen + 1)",
			filenameLen: 256,
			expectedErr: ErrFilenameTooLong,
		},
		{
			name:        "Filename length 65535 (uint16 max)",
			filenameLen: 65535,
			expectedErr: ErrFilenameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			buf.WriteByte(ProtocolVersion1)
			buf.WriteByte(MsgTypeFileTransfer)

			lenBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(lenBytes, tt.filenameLen)
			buf.Write(lenBytes)

			if tt.filenameLen > 0 && tt.filenameLen <= MaxFilenameLen {
				filenameStr := strings.Repeat("a", int(tt.filenameLen))
				buf.WriteString(filenameStr)

				sizeBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(sizeBytes, 1024)
				buf.Write(sizeBytes)

				checksum := [32]byte{1, 2, 3}
				buf.Write(checksum[:])
			}

			var h Header
			err := h.ReadFrom(buf)

			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(h.Filename) != int(tt.filenameLen) {
					t.Fatalf("expected filename length %d, got %d", tt.filenameLen, len(h.Filename))
				}
			}
		})
	}
}

func TestHeaderWriteTo_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		expectedErr error
	}{
		{
			name:        "Empty filename",
			filename:    "",
			expectedErr: ErrInvalidFilename,
		},
		{
			name:        "Filename length 255",
			filename:    strings.Repeat("b", 255),
			expectedErr: nil,
		},
		{
			name:        "Filename length 256",
			filename:    strings.Repeat("b", 256),
			expectedErr: ErrFilenameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
				FileSize: 2048,
				Checksum: [32]byte{4, 5, 6},
			}

			buf := new(bytes.Buffer)
			err := h.WriteTo(buf)

			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	origHeader := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_file.txt",
		FileSize: 12345,
		Checksum: [32]byte{0xde, 0xad, 0xbe, 0xef},
	}

	buf := new(bytes.Buffer)
	if err := origHeader.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(buf); err != nil {
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
		t.Errorf("Checksum mismatch: expected %v, got %v", origHeader.Checksum, readHeader.Checksum)
	}
}

func TestHeaderReadFrom_ZeroAllocationsOnMalformed(t *testing.T) {
	malformedFrames := []struct {
		name        string
		filenameLen uint16
		expectedErr error
	}{
		{"Length 0", 0, ErrInvalidFilename},
		{"Length 256", 256, ErrFilenameTooLong},
		{"Length 65535", 65535, ErrFilenameTooLong},
	}

	for _, tt := range malformedFrames {
		t.Run(tt.name, func(t *testing.T) {
			frame := make([]byte, 4)
			frame[0] = ProtocolVersion1
			frame[1] = MsgTypeFileTransfer
			binary.BigEndian.PutUint16(frame[2:4], tt.filenameLen)

			r := bytes.NewReader(frame)

			var err error
			var h Header
			allocs := testing.AllocsPerRun(100, func() {
				r.Reset(frame)
				err = h.ReadFrom(r)
			})

			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
			}

			if allocs > 0 {
				t.Fatalf("expected 0 allocations for malformed frame %s, got %f", tt.name, allocs)
			}
		})
	}
}
