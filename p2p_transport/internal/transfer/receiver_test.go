package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedBase string
		expectErr    bool
	}{
		{
			name:         "Standard filename",
			input:        "document.pdf",
			expectedBase: "document.pdf",
			expectErr:    false,
		},
		{
			name:         "Simple relative path traversal",
			input:        "../file.txt",
			expectedBase: "file.txt",
			expectErr:    false,
		},
		{
			name:         "Nested relative path traversal",
			input:        "../../etc/passwd",
			expectedBase: "passwd",
			expectErr:    false,
		},
		{
			name:         "Windows backslash path traversal",
			input:        "..\\..\\config\\identity.key",
			expectedBase: "identity.key",
			expectErr:    false,
		},
		{
			name:         "Absolute POSIX path",
			input:        "/tmp/file.txt",
			expectedBase: "file.txt",
			expectErr:    false,
		},
		{
			name:         "Absolute Windows path",
			input:        "C:\\Windows\\System32\\cmd.exe",
			expectedBase: "cmd.exe",
			expectErr:    false,
		},
		{
			name:         "Subdirectory path",
			input:        "downloads/sub/test.png",
			expectedBase: "test.png",
			expectErr:    false,
		},
		{
			name:      "Empty filename",
			input:     "",
			expectErr: true,
		},
		{
			name:      "Dot directory",
			input:     ".",
			expectErr: true,
		},
		{
			name:      "Double dot directory",
			input:     "..",
			expectErr: true,
		},
		{
			name:      "Root slash",
			input:     "/",
			expectErr: true,
		},
		{
			name:      "Backslash root",
			input:     "\\",
			expectErr: true,
		},
		{
			name:      "Multiple dots traversal end",
			input:     "../../",
			expectErr: true,
		},
		{
			name:      "Whitespace only",
			input:     "   ",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sanitizeFilename(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil (result: %q)", tc.input, got)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for input %q: %v", tc.input, err)
				}
				if got != tc.expectedBase {
					t.Errorf("sanitizeFilename(%q) = %q; want %q", tc.input, got, tc.expectedBase)
				}
				// Verify target path containment inside "downloads"
				downloadsDir := "downloads"
				outPath := filepath.Join(downloadsDir, got)
				cleanPath := filepath.Clean(outPath)
				if !strings.HasPrefix(cleanPath, downloadsDir+string(filepath.Separator)) && cleanPath != downloadsDir {
					t.Errorf("path %q escaped download directory: %q", tc.input, cleanPath)
				}
			}
		})
	}
}

func setupMockNetwork(t *testing.T) (host.Host, host.Host) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestReceive_PathTraversalSanitization(t *testing.T) {
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origWd)

	h1, h2 := setupMockNetwork(t)

	fileData := []byte("secret content for transfer test")
	checksum := sha256.Sum256(fileData)

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "../../escaped_file.txt",
		FileSize: uint64(len(fileData)),
		Checksum: checksum,
	}

	protocolID := protocol.ID("/test/1.0.0")

	errChan := make(chan error, 1)
	h2.SetStreamHandler(protocolID, func(s network.Stream) {
		errChan <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocolID)
	if err != nil {
		t.Fatal(err)
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(s, bytes.NewReader(fileData)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	recErr := <-errChan
	if recErr != nil {
		t.Fatalf("Receive failed unexpectedly: %v", recErr)
	}

	// Verify file is placed in "downloads/escaped_file.txt"
	expectedPath := filepath.Join("downloads", "escaped_file.txt")
	content, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read expected output file %s: %v", expectedPath, err)
	}
	if !bytes.Equal(content, fileData) {
		t.Fatalf("File content mismatch. Expected %q, got %q", fileData, content)
	}

	// Verify that "../escaped_file.txt" was NOT created
	if _, err := os.Stat("escaped_file.txt"); !os.IsNotExist(err) {
		t.Fatalf("File escaped downloads directory!")
	}
}

func TestReceive_InvalidFilenameRejected(t *testing.T) {
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origWd)

	h1, h2 := setupMockNetwork(t)

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "..",
		FileSize: 10,
		Checksum: [32]byte{},
	}

	protocolID := protocol.ID("/test/1.0.0")

	errChan := make(chan error, 1)
	h2.SetStreamHandler(protocolID, func(s network.Stream) {
		errChan <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocolID)
	if err != nil {
		t.Fatal(err)
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatal(err)
	}
	s.Close()

	recErr := <-errChan
	if recErr == nil {
		t.Fatalf("Expected error for invalid filename '..', got nil")
	}
}
