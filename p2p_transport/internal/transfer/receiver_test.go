package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockStream struct {
	network.Stream
	r      io.Reader
	closed bool
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	return m.r.Read(p)
}

func (m *mockStream) Close() error {
	m.closed = true
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

type mockConn struct {
	network.Conn
}

func (c *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

func (c *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

func TestSanitizeAndValidatePath(t *testing.T) {
	downloadsDir := "downloads"

	tests := []struct {
		name         string
		input        string
		expectedPath string
		expectError  bool
	}{
		{
			name:         "Standard simple filename",
			input:        "test.txt",
			expectedPath: filepath.Clean("downloads/test.txt"),
			expectError:  false,
		},
		{
			name:         "Standard video filename",
			input:        "video.mp4",
			expectedPath: filepath.Clean("downloads/video.mp4"),
			expectError:  false,
		},
		{
			name:         "Unix path traversal",
			input:        "../../etc/passwd",
			expectedPath: filepath.Clean("downloads/passwd"),
			expectError:  false,
		},
		{
			name:         "Nested Unix path traversal",
			input:        "downloads/../../secret.txt",
			expectedPath: filepath.Clean("downloads/secret.txt"),
			expectError:  false,
		},
		{
			name:         "Windows path traversal",
			input:        "..\\..\\Windows\\System32\\cmd.exe",
			expectedPath: filepath.Clean("downloads/cmd.exe"),
			expectError:  false,
		},
		{
			name:         "Absolute Unix path",
			input:        "/etc/shadow",
			expectedPath: filepath.Clean("downloads/shadow"),
			expectError:  false,
		},
		{
			name:         "Absolute Windows path",
			input:        "C:\\Windows\\System32\\cmd.exe",
			expectedPath: filepath.Clean("downloads/cmd.exe"),
			expectError:  false,
		},
		{
			name:        "Empty string filename",
			input:       "",
			expectError: true,
		},
		{
			name:        "Whitespace filename",
			input:       "   ",
			expectError: true,
		},
		{
			name:        "Dot directory",
			input:       ".",
			expectError: true,
		},
		{
			name:        "Dot dot directory",
			input:       "..",
			expectError: true,
		},
		{
			name:        "Root slash",
			input:       "/",
			expectError: true,
		},
		{
			name:        "Root backslash",
			input:       "\\",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := SanitizeAndValidatePath(tt.input, downloadsDir)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error for input %q, got nil (path: %s)", tt.input, gotPath)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for input %q: %v", tt.input, err)
				}
				if gotPath != tt.expectedPath {
					t.Errorf("expected path %q, got %q", tt.expectedPath, gotPath)
				}
			}
		})
	}
}

func TestReceive_InvalidAndTraversalHeaders(t *testing.T) {
	invalidFilenames := []string{"", "..", ".", "/", "\\", "   "}

	for _, filename := range invalidFilenames {
		t.Run("Filename_"+filename, func(t *testing.T) {
			hdr := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: filename,
				FileSize: 0,
			}
			var buf bytes.Buffer
			if err := hdr.WriteTo(&buf); err != nil {
				t.Fatalf("failed to write header: %v", err)
			}

			stream := &mockStream{r: &buf}
			err := Receive(stream)
			if err == nil {
				t.Errorf("expected Receive to fail for filename %q, but got nil", filename)
			}
			if !stream.closed {
				t.Errorf("expected stream to be closed after error")
			}
		})
	}
}

func TestReceive_SanitizesTraversalFilename(t *testing.T) {
	tempDir := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to chdir to tempDir: %v", err)
	}
	defer os.Chdir(origWd)

	fileData := []byte("hello world")
	checksum := sha256.Sum256(fileData)

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "../../traversal_test.txt",
		FileSize: uint64(len(fileData)),
		Checksum: checksum,
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(fileData)

	stream := &mockStream{r: &buf}
	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected Receive to succeed with sanitized filename, got: %v", err)
	}

	// Verify file was written to downloads/traversal_test.txt and NOT outside downloads
	expectedFilePath := filepath.Join("downloads", "traversal_test.txt")
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("expected file at %s, but it was not created", expectedFilePath)
	}

	outsidePath := filepath.Join("..", "traversal_test.txt")
	if _, err := os.Stat(outsidePath); !os.IsNotExist(err) {
		t.Errorf("file created outside downloads at %s!", outsidePath)
	}
}
