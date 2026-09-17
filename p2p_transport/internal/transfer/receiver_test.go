package transfer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/transfer"
)

func TestSanitizeAndValidatePath(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		rawFilename string
		wantBase    string
		wantErr     bool
	}{
		{
			name:        "Standard valid filename",
			rawFilename: "document.pdf",
			wantBase:    "document.pdf",
			wantErr:     false,
		},
		{
			name:        "Nested relative UNIX path",
			rawFilename: "folder/subfolder/file.txt",
			wantBase:    "file.txt",
			wantErr:     false,
		},
		{
			name:        "UNIX path traversal",
			rawFilename: "../../etc/passwd",
			wantBase:    "passwd",
			wantErr:     false,
		},
		{
			name:        "Absolute UNIX path",
			rawFilename: "/etc/passwd",
			wantBase:    "passwd",
			wantErr:     false,
		},
		{
			name:        "Windows path traversal",
			rawFilename: "..\\..\\Windows\\System32\\cmd.exe",
			wantBase:    "cmd.exe",
			wantErr:     false,
		},
		{
			name:        "Absolute Windows path",
			rawFilename: "C:\\Windows\\System32\\cmd.exe",
			wantBase:    "cmd.exe",
			wantErr:     false,
		},
		{
			name:        "Windows relative path with backslashes",
			rawFilename: "folder\\subfolder\\photo.jpg",
			wantBase:    "photo.jpg",
			wantErr:     false,
		},
		{
			name:        "Empty string",
			rawFilename: "",
			wantErr:     true,
		},
		{
			name:        "Single dot",
			rawFilename: ".",
			wantErr:     true,
		},
		{
			name:        "Double dot",
			rawFilename: "..",
			wantErr:     true,
		},
		{
			name:        "UNIX root directory separator",
			rawFilename: "/",
			wantErr:     true,
		},
		{
			name:        "Windows backslash directory separator",
			rawFilename: "\\",
			wantErr:     true,
		},
		{
			name:        "UNIX directory traversal trailing slash",
			rawFilename: "../../",
			wantErr:     true,
		},
		{
			name:        "Windows directory traversal trailing backslash",
			rawFilename: "..\\..\\",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := transfer.SanitizeAndValidatePath(tempDir, tt.rawFilename)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizeAndValidatePath(%q) error = %v, wantErr %v", tt.rawFilename, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				expectedPath := filepath.Join(filepath.Clean(tempDir), tt.wantBase)
				if gotPath != expectedPath {
					t.Errorf("SanitizeAndValidatePath(%q) = %q, want %q", tt.rawFilename, gotPath, expectedPath)
				}
				// Verify target stays bounded within tempDir
				rel, err := filepath.Rel(filepath.Clean(tempDir), gotPath)
				if err != nil || rel == "." || rel == ".." || filepath.Dir(gotPath) != filepath.Clean(tempDir) {
					t.Errorf("SanitizeAndValidatePath(%q) path escaping check failed, rel = %q", tt.rawFilename, rel)
				}
			}
		})
	}
}

func TestReceive_EndToEndAndTraversal(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	tempDir := t.TempDir()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origDir)

	ctx := context.Background()

	t.Run("Receive valid file transfer", func(t *testing.T) {
		errCh := make(chan error, 1)
		h1.SetStreamHandler("test-proto-1", func(s network.Stream) {
			errCh <- transfer.Receive(s)
		})

		s2, err := h2.NewStream(ctx, h1.ID(), "test-proto-1")
		if err != nil {
			t.Fatal(err)
		}

		payload := []byte("Hello, world!")
		hasher := sha256.New()
		hasher.Write(payload)
		var checksum [32]byte
		copy(checksum[:], hasher.Sum(nil))

		hdr := transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: "hello.txt",
			FileSize: uint64(len(payload)),
			Checksum: checksum,
		}

		if err := hdr.WriteTo(s2); err != nil {
			t.Fatalf("WriteTo failed: %v", err)
		}
		if _, err := s2.Write(payload); err != nil {
			t.Fatalf("Write payload failed: %v", err)
		}

		if err := <-errCh; err != nil {
			t.Fatalf("Receive failed: %v", err)
		}

		expectedPath := filepath.Join("downloads", "hello.txt")
		content, err := os.ReadFile(expectedPath)
		if err != nil {
			t.Fatalf("Failed to read received file: %v", err)
		}
		if !bytes.Equal(content, payload) {
			t.Fatalf("Content mismatch: got %s, want %s", string(content), string(payload))
		}
	})

	t.Run("Receive traversal payload strips directory components", func(t *testing.T) {
		errCh := make(chan error, 1)
		h1.SetStreamHandler("test-proto-2", func(s network.Stream) {
			errCh <- transfer.Receive(s)
		})

		s2, err := h2.NewStream(ctx, h1.ID(), "test-proto-2")
		if err != nil {
			t.Fatal(err)
		}

		payload := []byte("Malicious payload content")
		hasher := sha256.New()
		hasher.Write(payload)
		var checksum [32]byte
		copy(checksum[:], hasher.Sum(nil))

		hdr := transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: "../../etc/shadow",
			FileSize: uint64(len(payload)),
			Checksum: checksum,
		}

		if err := hdr.WriteTo(s2); err != nil {
			t.Fatalf("WriteTo failed: %v", err)
		}
		if _, err := s2.Write(payload); err != nil {
			t.Fatalf("Write payload failed: %v", err)
		}

		if err := <-errCh; err != nil {
			t.Fatalf("Receive failed for sanitized traversal filename: %v", err)
		}

		// Ensure shadow was saved inside downloads/shadow and NOT /etc/shadow
		expectedPath := filepath.Join("downloads", "shadow")
		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Fatalf("Expected file at %s, but not found", expectedPath)
		}
	})

	t.Run("Receive invalid header filename evaluating to directory separator aborts stream", func(t *testing.T) {
		errCh := make(chan error, 1)
		h1.SetStreamHandler("test-proto-3", func(s network.Stream) {
			errCh <- transfer.Receive(s)
		})

		s2, err := h2.NewStream(ctx, h1.ID(), "test-proto-3")
		if err != nil {
			t.Fatal(err)
		}

		hdr := transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: "../../",
			FileSize: 100,
			Checksum: [32]byte{},
		}

		if err := hdr.WriteTo(s2); err != nil {
			t.Fatalf("WriteTo failed: %v", err)
		}

		recvErr := <-errCh
		if recvErr == nil {
			t.Fatalf("Expected error when filename evaluates to directory separator, got nil")
		}
	})
}
