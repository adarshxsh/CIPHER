package transfer

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libprotocol "github.com/libp2p/go-libp2p/core/protocol"
)

func TestSanitizeAndValidatePath(t *testing.T) {
	tempDir := t.TempDir()
	downloadsDir := filepath.Join(tempDir, "downloads")
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		t.Fatalf("failed to create downloads dir: %v", err)
	}

	tests := []struct {
		name         string
		rawFilename  string
		expectedPath string
	}{
		{
			name:         "clean filename",
			rawFilename:  "hello.txt",
			expectedPath: filepath.Join(downloadsDir, "hello.txt"),
		},
		{
			name:         "POSIX relative path traversal",
			rawFilename:  "../../etc/passwd",
			expectedPath: filepath.Join(downloadsDir, "passwd"),
		},
		{
			name:         "POSIX absolute path traversal",
			rawFilename:  "/etc/passwd",
			expectedPath: filepath.Join(downloadsDir, "passwd"),
		},
		{
			name:         "Windows relative path traversal",
			rawFilename:  "..\\..\\windows\\system32\\cmd.exe",
			expectedPath: filepath.Join(downloadsDir, "cmd.exe"),
		},
		{
			name:         "Windows absolute path traversal",
			rawFilename:  "C:\\Windows\\System32\\cmd.exe",
			expectedPath: filepath.Join(downloadsDir, "cmd.exe"),
		},
		{
			name:         "nested subdirectory path",
			rawFilename:  "subdir/nested/file.png",
			expectedPath: filepath.Join(downloadsDir, "file.png"),
		},
		{
			name:         "empty filename fallback",
			rawFilename:  "",
			expectedPath: filepath.Join(downloadsDir, "downloaded_file"),
		},
		{
			name:         "dot filename fallback",
			rawFilename:  ".",
			expectedPath: filepath.Join(downloadsDir, "downloaded_file"),
		},
		{
			name:         "parent dot filename fallback",
			rawFilename:  "..",
			expectedPath: filepath.Join(downloadsDir, "downloaded_file"),
		},
		{
			name:         "root slash fallback",
			rawFilename:  "/",
			expectedPath: filepath.Join(downloadsDir, "downloaded_file"),
		},
		{
			name:         "windows root backslash fallback",
			rawFilename:  "\\",
			expectedPath: filepath.Join(downloadsDir, "downloaded_file"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SanitizeAndValidatePath(downloadsDir, tt.rawFilename)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expectedPath {
				t.Errorf("SanitizeAndValidatePath(%q, %q) = %q, want %q", downloadsDir, tt.rawFilename, got, tt.expectedPath)
			}

			// Verify absolute path containment
			absDownloads, err := filepath.Abs(downloadsDir)
			if err != nil {
				t.Fatalf("failed to get abs downloads: %v", err)
			}
			absGot, err := filepath.Abs(got)
			if err != nil {
				t.Fatalf("failed to get abs got: %v", err)
			}

			rel, err := filepath.Rel(absDownloads, absGot)
			if err != nil {
				t.Fatalf("filepath.Rel failed: %v", err)
			}

			relClean := filepath.Clean(rel)
			if relClean == ".." || relClean == "." || filepath.IsAbs(relClean) {
				t.Errorf("path %q escaped downloads dir %q (rel: %q)", got, downloadsDir, rel)
			}
		})
	}
}

func TestReceive_SafeAndTraversalTransfers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}
	defer h2.Close()

	info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}
	if err := h1.Connect(ctx, info); err != nil {
		t.Fatalf("failed to connect h1 to h2: %v", err)
	}

	// Change directory to temporary directory so Receive writes downloads into isolated temp folder
	tempDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	defer os.Chdir(origDir)

	testData := []byte("Hello, secure CIPHER file transfer!")
	hasher := sha256.New()
	hasher.Write(testData)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	tests := []struct {
		name             string
		wireFilename     string
		expectedLocation string
	}{
		{
			name:             "clean filename transfer",
			wireFilename:     "safe_doc.txt",
			expectedLocation: filepath.Join("downloads", "safe_doc.txt"),
		},
		{
			name:             "traversal filename transfer",
			wireFilename:     "../../unsafe_doc.txt",
			expectedLocation: filepath.Join("downloads", "unsafe_doc.txt"),
		},
		{
			name:             "dot filename transfer",
			wireFilename:     "..",
			expectedLocation: filepath.Join("downloads", "downloaded_file"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocolID := libprotocol.ID("/test/transfer/1.0.0")

			done := make(chan error, 1)
			h2.SetStreamHandler(protocolID, func(s network.Stream) {
				done <- Receive(s)
			})

			stream, err := h1.NewStream(ctx, h2.ID(), protocolID)
			if err != nil {
				t.Fatalf("failed to open stream: %v", err)
			}

			// Send header with test wireFilename
			header := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.wireFilename,
				FileSize: uint64(len(testData)),
				Checksum: checksum,
			}
			if err := header.WriteTo(stream); err != nil {
				t.Fatalf("failed to write header: %v", err)
			}

			// Send payload
			if _, err := stream.Write(testData); err != nil {
				t.Fatalf("failed to write data: %v", err)
			}

			err = <-done
			if err != nil {
				t.Fatalf("Receive returned unexpected error: %v", err)
			}

			// Check expected file location
			data, err := os.ReadFile(tt.expectedLocation)
			if err != nil {
				t.Fatalf("failed to read expected file at %s: %v", tt.expectedLocation, err)
			}

			if string(data) != string(testData) {
				t.Errorf("file contents mismatch: got %q, want %q", string(data), string(testData))
			}

			// Ensure file was not created in root directory (outside downloads)
			if tt.wireFilename == "../../unsafe_doc.txt" {
				if _, err := os.Stat("unsafe_doc.txt"); err == nil {
					t.Errorf("security flaw: unsafe_doc.txt was written outside downloads folder!")
				}
			}
		})
	}
}
