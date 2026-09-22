package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Plain filename",
			input:    "document.pdf",
			expected: "document.pdf",
		},
		{
			name:     "Unix relative path",
			input:    "../document.pdf",
			expected: "document.pdf",
		},
		{
			name:     "Unix deep path traversal",
			input:    "../../../../etc/passwd",
			expected: "passwd",
		},
		{
			name:     "Windows deep path traversal",
			input:    "..\\..\\..\\Windows\\System32\\cmd.exe",
			expected: "cmd.exe",
		},
		{
			name:     "Absolute Unix path",
			input:    "/var/log/system.log",
			expected: "system.log",
		},
		{
			name:     "Absolute Windows path",
			input:    "C:\\Users\\Public\\Downloads\\data.csv",
			expected: "data.csv",
		},
		{
			name:     "Single dot",
			input:    ".",
			expected: DefaultFilename,
		},
		{
			name:     "Double dot",
			input:    "..",
			expected: DefaultFilename,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: DefaultFilename,
		},
		{
			name:     "Unix root",
			input:    "/",
			expected: DefaultFilename,
		},
		{
			name:     "Windows root backslash",
			input:    "\\",
			expected: DefaultFilename,
		},
		{
			name:     "Whitespace only",
			input:    "   ",
			expected: DefaultFilename,
		},
		{
			name:     "Dot slash",
			input:    "./",
			expected: DefaultFilename,
		},
		{
			name:     "Dot dot slash",
			input:    "../",
			expected: DefaultFilename,
		},
		{
			name:     "Dot dot backslash",
			input:    "..\\",
			expected: DefaultFilename,
		},
		{
			name:     "Filename with embedded null byte",
			input:    "payload\x00.exe",
			expected: "payload.exe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeFilename(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeFilename(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestValidateDestinationPath(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "transfer_test_downloads_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cleanTempDir, err := filepath.Abs(filepath.Clean(tempDir))
	if err != nil {
		t.Fatalf("failed to resolve abs temp dir: %v", err)
	}

	t.Run("Valid filename inside downloads", func(t *testing.T) {
		destPath, err := ValidateDestinationPath(cleanTempDir, "sample.txt")
		if err != nil {
			t.Fatalf("unexpected error for valid filename: %v", err)
		}
		expected := filepath.Join(cleanTempDir, "sample.txt")
		if destPath != expected {
			t.Errorf("got destPath %q; want %q", destPath, expected)
		}
	})

	t.Run("Traversal filename auto-remediated to base inside downloads", func(t *testing.T) {
		destPath, err := ValidateDestinationPath(cleanTempDir, "../../etc/passwd")
		if err != nil {
			t.Fatalf("unexpected error for traversal filename: %v", err)
		}
		expected := filepath.Join(cleanTempDir, "passwd")
		if destPath != expected {
			t.Errorf("got destPath %q; want %q", destPath, expected)
		}
	})
}

type testConn struct {
	network.Conn
	remotePeer peer.ID
}

func (c *testConn) RemotePeer() peer.ID {
	return c.remotePeer
}

func (c *testConn) RemoteMultiaddr() ma.Multiaddr {
	m, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return m
}

type testStream struct {
	network.Stream
	netConn    net.Conn
	remotePeer peer.ID
}

func (s *testStream) Read(p []byte) (int, error) {
	return s.netConn.Read(p)
}

func (s *testStream) Write(p []byte) (int, error) {
	return s.netConn.Write(p)
}

func (s *testStream) Close() error {
	return s.netConn.Close()
}

func (s *testStream) Conn() network.Conn {
	return &testConn{remotePeer: s.remotePeer}
}

func newMockStreamPair() (network.Stream, network.Stream) {
	clientConn, serverConn := net.Pipe()
	return &testStream{netConn: clientConn, remotePeer: "test-peer"}, &testStream{netConn: serverConn, remotePeer: "test-peer"}
}

func TestReceive_TraversalPrevention(t *testing.T) {
	// Change working directory to a temporary directory so downloads/ is created inside temp
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	tempWd, err := os.MkdirTemp("", "p2p_receive_test_*")
	if err != nil {
		t.Fatalf("failed to create temp wd: %v", err)
	}
	defer func() {
		os.Chdir(origWd)
		os.RemoveAll(tempWd)
	}()

	if err := os.Chdir(tempWd); err != nil {
		t.Fatalf("failed to chdir to tempWd: %v", err)
	}

	payload := []byte("hello safe transfer")
	hash := sha256.Sum256(payload)

	testCases := []struct {
		name             string
		wireFilename     string
		expectedFileName string
	}{
		{
			name:             "Path traversal attempting escape",
			wireFilename:     "../../outside_secret.txt",
			expectedFileName: "outside_secret.txt",
		},
		{
			name:             "Absolute path attempting root write",
			wireFilename:     "/etc/evil.conf",
			expectedFileName: "evil.conf",
		},
		{
			name:             "Dot filename",
			wireFilename:     ".",
			expectedFileName: DefaultFilename,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sSender, sReceiver := newMockStreamPair()

			hdr := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tc.wireFilename,
				FileSize: uint64(len(payload)),
				Checksum: hash,
			}

			errChan := make(chan error, 1)
			go func() {
				errChan <- Receive(sReceiver)
			}()

			go func() {
				defer sSender.Close()
				if err := hdr.WriteTo(sSender); err != nil {
					t.Errorf("failed to write header: %v", err)
					return
				}
				io.Copy(sSender, bytes.NewReader(payload))
			}()

			if err := <-errChan; err != nil {
				t.Fatalf("Receive failed unexpectedly: %v", err)
			}

			// Verify file exists inside downloads/ and NOT outside
			expectedPath := filepath.Join("downloads", tc.expectedFileName)
			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Errorf("expected file %q was not created inside downloads directory", expectedPath)
			}

			// Verify outside_secret.txt or evil.conf was NOT created in root/parent dir
			outsidePath := filepath.Join("..", tc.expectedFileName)
			if _, err := os.Stat(outsidePath); err == nil {
				t.Errorf("SECURITY RISK: file was created outside downloads directory at %q", outsidePath)
			}
		})
	}
}
