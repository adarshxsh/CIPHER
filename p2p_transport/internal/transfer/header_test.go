package transfer

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestMaxFilenameLenConstant(t *testing.T) {
	if MaxFilenameLen != 255 {
		t.Fatalf("expected MaxFilenameLen to be 255, got %d", MaxFilenameLen)
	}
}

func TestHeaderWriteToBoundaryConditions(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		expectErr bool
	}{
		{
			name:      "zero length filename",
			filename:  "",
			expectErr: true,
		},
		{
			name:      "1 byte filename",
			filename:  "a",
			expectErr: false,
		},
		{
			name:      "255 byte filename",
			filename:  strings.Repeat("a", 255),
			expectErr: false,
		},
		{
			name:      "256 byte filename",
			filename:  strings.Repeat("a", 256),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
				FileSize: 100,
				Checksum: [32]byte{},
			}
			var buf bytes.Buffer
			err := h.WriteTo(&buf)

			if tt.expectErr && err == nil {
				t.Errorf("expected error for filename length %d, got nil", len(tt.filename))
			}
			if !tt.expectErr && err != nil {
				t.Errorf("unexpected error for filename length %d: %v", len(tt.filename), err)
			}
		})
	}
}

func TestHeaderReadFromBoundaryConditions(t *testing.T) {
	// Test cases with explicit boundary lengths: 0, 1, 255, 256, 65535
	boundaryLengths := []struct {
		name      string
		length    uint16
		expectErr bool
	}{
		{
			name:      "length 0",
			length:    0,
			expectErr: true,
		},
		{
			name:      "length 1",
			length:    1,
			expectErr: false,
		},
		{
			name:      "length 255",
			length:    255,
			expectErr: false,
		},
		{
			name:      "length 256",
			length:    256,
			expectErr: true,
		},
		{
			name:      "length 65535",
			length:    65535,
			expectErr: true,
		},
	}

	for _, tt := range boundaryLengths {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			// Write Version (1 byte)
			buf.WriteByte(ProtocolVersion1)
			// Write Type (1 byte)
			buf.WriteByte(MsgTypeFileTransfer)
			// Write Filename Length (2 bytes)
			binary.Write(&buf, binary.BigEndian, tt.length)

			if !tt.expectErr {
				// Provide payload for valid lengths
				buf.WriteString(strings.Repeat("x", int(tt.length)))
				// FileSize (8 bytes)
				binary.Write(&buf, binary.BigEndian, uint64(500))
				// Checksum (32 bytes)
				buf.Write(make([]byte, 32))
			} else {
				// For invalid lengths, do NOT write any payload bytes.
				// If ReadFrom attempts to read the payload, it will fail or hang/EOF.
				// We want to verify ReadFrom fails before reading payload.
			}

			var decoded Header
			err := decoded.ReadFrom(&buf)

			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error for length %d, got nil", tt.length)
				}
				if !strings.Contains(err.Error(), "invalid filename length") {
					t.Fatalf("expected invalid filename length error for length %d, got: %v", tt.length, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for length %d: %v", tt.length, err)
				}
				if len(decoded.Filename) != int(tt.length) {
					t.Fatalf("expected filename length %d, got %d", tt.length, len(decoded.Filename))
				}
			}
		})
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	original := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "valid_filename.txt",
		FileSize: 123456789,
		Checksum: [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	var buf bytes.Buffer
	if err := original.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var decoded Header
	if err := decoded.ReadFrom(&buf); err != nil {
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
}
