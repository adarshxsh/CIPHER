package transfer_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

func TestSanitizeFilename(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		rawFilename string
		wantBase    string
		expectError bool
	}{
		{
			name:        "Standard filename",
			rawFilename: "document.pdf",
			wantBase:    "document.pdf",
			expectError: false,
		},
		{
			name:        "Subdirectory path",
			rawFilename: "folder/subfolder/document.pdf",
			wantBase:    "document.pdf",
			expectError: false,
		},
		{
			name:        "POSIX path traversal payload",
			rawFilename: "../../etc/passwd",
			wantBase:    "passwd",
			expectError: false,
		},
		{
			name:        "Windows path traversal payload",
			rawFilename: "..\\..\\Windows\\System32\\cmd.exe",
			wantBase:    "cmd.exe",
			expectError: false,
		},
		{
			name:        "Absolute POSIX path",
			rawFilename: "/var/log/syslog",
			wantBase:    "syslog",
			expectError: false,
		},
		{
			name:        "Absolute Windows path",
			rawFilename: "C:\\Windows\\System32\\calc.exe",
			wantBase:    "calc.exe",
			expectError: false,
		},
		{
			name:        "Dot reference",
			rawFilename: ".",
			expectError: true,
		},
		{
			name:        "Dot-dot reference",
			rawFilename: "..",
			expectError: true,
		},
		{
			name:        "Trailing slash dot-dot",
			rawFilename: "dir/subdir/..",
			expectError: true,
		},
		{
			name:        "Empty filename",
			rawFilename: "",
			expectError: true,
		},
		{
			name:        "Whitespace filename",
			rawFilename: "   ",
			expectError: true,
		},
		{
			name:        "Root slash",
			rawFilename: "/",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := transfer.SanitizeFilename(tempDir, tt.rawFilename)
			if tt.expectError {
				if err == nil {
					t.Errorf("SanitizeFilename(%q) expected error, got nil (path: %q)", tt.rawFilename, gotPath)
				}
			} else {
				if err != nil {
					t.Fatalf("SanitizeFilename(%q) unexpected error: %v", tt.rawFilename, err)
				}
				expectedPath := filepath.Join(tempDir, tt.wantBase)
				if gotPath != expectedPath {
					t.Errorf("SanitizeFilename(%q) = %q; want %q", tt.rawFilename, gotPath, expectedPath)
				}
			}
		})
	}
}

func TestReceive_PathTraversalSanitization(t *testing.T) {
	// Create mock network
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	// Change working directory to temp dir so downloads/ is isolated
	workDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir to temp workdir: %v", err)
	}
	defer os.Chdir(origWd)

	testData := []byte("secret content to test receiver path sanitization")
	hasher := sha256.New()
	hasher.Write(testData)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	// Setup stream handler on h2
	var recvErr error
	recvDone := make(chan struct{})
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErr = transfer.Receive(s)
		close(recvDone)
	})

	// Open stream from h1 to h2
	ctx := context.Background()
	s, err := h1.NewStream(ctx, h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Send malicious header with relative path traversal
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "../../etc/malicious_payload.txt",
		FileSize: uint64(len(testData)),
		Checksum: checksum,
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	if _, err := s.Write(testData); err != nil {
		t.Fatalf("failed to write test payload: %v", err)
	}
	s.CloseWrite()

	<-recvDone
	if recvErr != nil {
		t.Fatalf("expected Receive to succeed safely, got error: %v", recvErr)
	}

	// Verify file was saved safely inside downloads/ as malicious_payload.txt
	expectedFilePath := filepath.Join(workDir, "downloads", "malicious_payload.txt")
	content, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("expected file at %s, failed to read: %v", expectedFilePath, err)
	}
	if !bytes.Equal(content, testData) {
		t.Errorf("file content mismatch: got %q, want %q", content, testData)
	}

	// Verify that NO file was created outside downloads
	outsidePath := filepath.Join(workDir, "etc", "malicious_payload.txt")
	if _, err := os.Stat(outsidePath); !os.IsNotExist(err) {
		t.Errorf("path traversal vulnerability! File was written outside downloads: %s", outsidePath)
	}
}

func TestReceive_InvalidDirectoryReference(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	workDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir to temp workdir: %v", err)
	}
	defer os.Chdir(origWd)

	var recvErr error
	recvDone := make(chan struct{})
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErr = transfer.Receive(s)
		close(recvDone)
	})

	ctx := context.Background()
	s, err := h1.NewStream(ctx, h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "..",
		FileSize: 10,
		Checksum: [32]byte{},
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	<-recvDone
	if recvErr == nil {
		t.Fatalf("expected error when receiving header with filename '..', got nil")
	}
	if !strings.Contains(recvErr.Error(), "path validation failed") {
		t.Errorf("unexpected error message: %v", recvErr)
	}
}
