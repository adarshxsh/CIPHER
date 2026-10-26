package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
)

func TestSanitizeAndValidatePath(t *testing.T) {
	tempDir := t.TempDir()
	downloadsDir := filepath.Join(tempDir, "downloads")
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp downloads directory: %v", err)
	}

	absDownloadsDir, err := filepath.Abs(downloadsDir)
	if err != nil {
		t.Fatalf("Failed to get abs path: %v", err)
	}

	tests := []struct {
		name         string
		rawFilename  string
		expectError  bool
		expectedBase string
	}{
		{
			name:         "Legitimate filename",
			rawFilename:  "report.pdf",
			expectError:  false,
			expectedBase: "report.pdf",
		},
		{
			name:         "Legitimate filename with spaces",
			rawFilename:  "my report 2026.pdf",
			expectError:  false,
			expectedBase: "my report 2026.pdf",
		},
		{
			name:         "Relative path traversal POSIX",
			rawFilename:  "../../etc/passwd",
			expectError:  false,
			expectedBase: "passwd",
		},
		{
			name:         "Relative path traversal Windows",
			rawFilename:  "..\\..\\Windows\\System32\\cmd.exe",
			expectError:  false,
			expectedBase: "cmd.exe",
		},
		{
			name:         "Nested subdirectories POSIX",
			rawFilename:  "sub/dir/file.txt",
			expectError:  false,
			expectedBase: "file.txt",
		},
		{
			name:         "Nested subdirectories Windows",
			rawFilename:  "sub\\dir\\file.txt",
			expectError:  false,
			expectedBase: "file.txt",
		},
		{
			name:         "Absolute path POSIX",
			rawFilename:  "/etc/shadow",
			expectError:  false,
			expectedBase: "shadow",
		},
		{
			name:         "Absolute path Windows",
			rawFilename:  "C:\\Windows\\System32\\drivers\\etc\\hosts",
			expectError:  false,
			expectedBase: "hosts",
		},
		{
			name:        "Empty filename",
			rawFilename: "",
			expectError: true,
		},
		{
			name:        "Single dot",
			rawFilename: ".",
			expectError: true,
		},
		{
			name:        "Double dot",
			rawFilename: "..",
			expectError: true,
		},
		{
			name:        "POSIX separator only",
			rawFilename: "/",
			expectError: true,
		},
		{
			name:        "Multiple POSIX separators",
			rawFilename: "///",
			expectError: true,
		},
		{
			name:        "Windows separator only",
			rawFilename: "\\",
			expectError: true,
		},
		{
			name:        "Multiple Windows separators",
			rawFilename: "\\\\\\",
			expectError: true,
		},
		{
			name:        "Slash dot",
			rawFilename: "/.",
			expectError: true,
		},
		{
			name:        "Dot slash",
			rawFilename: "./",
			expectError: true,
		},
		{
			name:        "Slash dot dot",
			rawFilename: "/..",
			expectError: true,
		},
		{
			name:        "Backslash dot dot",
			rawFilename: "\\..",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPath, err := SanitizeAndValidatePath(downloadsDir, tc.rawFilename)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error for rawFilename %q, got nil (path: %q)", tc.rawFilename, gotPath)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for rawFilename %q: %v", tc.rawFilename, err)
					return
				}
				expectedPath := filepath.Join(downloadsDir, tc.expectedBase)
				if gotPath != expectedPath {
					t.Errorf("Path mismatch: expected %q, got %q", expectedPath, gotPath)
				}

				// Verify target path resides strictly inside downloads directory
				absGotPath, err := filepath.Abs(gotPath)
				if err != nil {
					t.Fatalf("Failed to get abs path for gotPath: %v", err)
				}

				rel, err := filepath.Rel(absDownloadsDir, absGotPath)
				if err != nil {
					t.Fatalf("Failed to compute rel path: %v", err)
				}

				if strings.HasPrefix(rel, "..") || rel == "." {
					t.Errorf("Target path %q escaped base directory %q (rel: %q)", absGotPath, absDownloadsDir, rel)
				}
			}
		})
	}
}

func setupMockStreamPair(t *testing.T) (host.Host, host.Host) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to generate peer h1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to generate peer h2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("Failed to link mocknet: %v", err)
	}
	return h1, h2
}

func TestReceive_EndToEnd_ValidAndPathTraversal(t *testing.T) {
	// Change working directory to a temporary folder during test to test downloads/ creation in isolation
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current wd: %v", err)
	}
	tempWd := t.TempDir()
	if err := os.Chdir(tempWd); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}
	defer func() {
		_ = os.Chdir(origDir)
	}()

	h1, h2 := setupMockStreamPair(t)

	// 1. Test Valid File Transfer
	t.Run("Valid Transfer", func(t *testing.T) {
		payload := []byte("Hello CIPHER secure file transfer!")
		checksum := sha256.Sum256(payload)

		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: "sample.txt",
			FileSize: uint64(len(payload)),
			Checksum: checksum,
		}

		errChan := make(chan error, 1)
		h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
			errChan <- Receive(s)
		})

		ctx := context.Background()
		s, err := h1.NewStream(ctx, h2.ID(), protocol.FileTransferProtocolID)
		if err != nil {
			t.Fatalf("Failed to open stream: %v", err)
		}

		if err := header.WriteTo(s); err != nil {
			t.Fatalf("Failed to write header: %v", err)
		}
		if _, err := s.Write(payload); err != nil {
			t.Fatalf("Failed to write payload: %v", err)
		}

		if err := <-errChan; err != nil {
			t.Fatalf("Receive failed unexpectedly: %v", err)
		}

		// Verify file exists in downloads/sample.txt
		receivedData, err := os.ReadFile("downloads/sample.txt")
		if err != nil {
			t.Fatalf("Failed to read received file: %v", err)
		}
		if !bytes.Equal(receivedData, payload) {
			t.Errorf("Content mismatch: expected %q, got %q", payload, receivedData)
		}
	})

	// 2. Test Path Traversal Payload header
	t.Run("Path Traversal Payload", func(t *testing.T) {
		payload := []byte("malicious content trying to write to /etc/passwd")
		checksum := sha256.Sum256(payload)

		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: "../../etc/passwd",
			FileSize: uint64(len(payload)),
			Checksum: checksum,
		}

		errChan := make(chan error, 1)
		h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
			errChan <- Receive(s)
		})

		ctx := context.Background()
		s, err := h1.NewStream(ctx, h2.ID(), protocol.FileTransferProtocolID)
		if err != nil {
			t.Fatalf("Failed to open stream: %v", err)
		}

		if err := header.WriteTo(s); err != nil {
			t.Fatalf("Failed to write header: %v", err)
		}
		if _, err := s.Write(payload); err != nil {
			t.Fatalf("Failed to write payload: %v", err)
		}

		if err := <-errChan; err != nil {
			t.Fatalf("Receive failed unexpectedly for sanitized path: %v", err)
		}

		// Verify file was NOT created outside downloads (e.g. etc/passwd does not exist in tempWd)
		if _, err := os.Stat("etc/passwd"); err == nil {
			t.Errorf("File was improperly created outside downloads directory!")
		}

		// Verify base filename "passwd" was created safely inside downloads/passwd
		receivedData, err := os.ReadFile("downloads/passwd")
		if err != nil {
			t.Fatalf("Expected file inside downloads/passwd, got error: %v", err)
		}
		if !bytes.Equal(receivedData, payload) {
			t.Errorf("Content mismatch: expected %q, got %q", payload, receivedData)
		}
	})

	// 3. Test Invalid Filename Header (dot / empty)
	t.Run("Invalid Dot Filename Header", func(t *testing.T) {
		payload := []byte("some payload")
		checksum := sha256.Sum256(payload)

		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: "..",
			FileSize: uint64(len(payload)),
			Checksum: checksum,
		}

		errChan := make(chan error, 1)
		h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
			errChan <- Receive(s)
		})

		ctx := context.Background()
		s, err := h1.NewStream(ctx, h2.ID(), protocol.FileTransferProtocolID)
		if err != nil {
			t.Fatalf("Failed to open stream: %v", err)
		}

		if err := header.WriteTo(s); err != nil {
			t.Fatalf("Failed to write header: %v", err)
		}
		if _, err := s.Write(payload); err != nil {
			t.Fatalf("Failed to write payload: %v", err)
		}

		err = <-errChan
		if err == nil {
			t.Errorf("Expected Receive to return error for filename %q, got nil", header.Filename)
		} else if !strings.Contains(err.Error(), "invalid transfer path") {
			t.Errorf("Expected invalid transfer path error, got: %v", err)
		}
	})
}
