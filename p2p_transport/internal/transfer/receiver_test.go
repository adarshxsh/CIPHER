package transfer

import (
	"bytes"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockStream struct {
	network.Stream
	netConn net.Conn
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.netConn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.netConn.Write(p)
}

func (m *mockStream) Close() error {
	return m.netConn.Close()
}

func (m *mockStream) Reset() error {
	return m.netConn.Close()
}

func (m *mockStream) ResetWithError(errCode network.StreamErrorCode) error {
	return m.netConn.Close()
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

type mockConn struct {
	network.Conn
}

func (mc *mockConn) RemotePeer() peer.ID {
	return "12D3KooWTestRemote"
}

func (mc *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	return nil
}

func TestHeaderValidation(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{"Valid standard filename", "document.pdf", false},
		{"Valid filename with subpath", "sub/folder/file.txt", false},
		{"Empty filename", "", true},
		{"Null byte in filename", "file\x00.txt", true},
		{"Control char newline", "file\n.txt", true},
		{"Control char carriage return", "file\r.txt", true},
		{"Control char tab", "file\t.txt", true},
		{"Control char bell", "file\x07.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Header{
				Version:  ProtocolVersion1,
				Type:     MsgTypeFileTransfer,
				Filename: tt.filename,
				FileSize: 100,
			}
			err := h.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Header.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test ReadFrom behavior
			var buf bytes.Buffer
			_ = h.WriteTo(&buf)
			var hRead Header
			readErr := hRead.ReadFrom(&buf)
			if (readErr != nil) != tt.wantErr {
				t.Errorf("Header.ReadFrom() error = %v, wantErr %v", readErr, tt.wantErr)
			}
		})
	}
}

func TestSanitizeAndValidatePath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_downloads_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	absTmpDir, err := filepath.Abs(tmpDir)
	if err != nil {
		t.Fatalf("Failed to resolve abs temp dir: %v", err)
	}

	tests := []struct {
		name         string
		filename     string
		wantBaseName string
		wantErr      bool
	}{
		{"Standard filename", "document.pdf", "document.pdf", false},
		{"Relative path traversal unix", "../file.txt", "file.txt", false},
		{"Deep relative traversal unix", "../../../etc/shadow", "shadow", false},
		{"Absolute path unix", "/etc/passwd", "passwd", false},
		{"Windows backslash traversal", "..\\..\\Windows\\System32\\config", "config", false},
		{"Windows drive letter prefix", "C:\\Windows\\System32\\cmd.exe", "cmd.exe", false},
		{"Nested directory payload", "folder/subfolder/data.bin", "data.bin", false},
		{"Dot path", ".", "", true},
		{"Double dot path", "..", "", true},
		{"Root slash path", "/", "", true},
		{"Backslash path", "\\", "", true},
		{"Empty path", "", "", true},
		{"Whitespace path", "   ", "", true},
		{"Null byte path", "payload\x00.exe", "", true},
		{"Control char path", "payload\n.exe", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outPath, err := SanitizeAndValidatePath(tt.filename, tmpDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizeAndValidatePath(%q) error = %v, wantErr %v", tt.filename, err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				expectedPath := filepath.Join(absTmpDir, tt.wantBaseName)
				if outPath != expectedPath {
					t.Errorf("SanitizeAndValidatePath(%q) = %q, want %q", tt.filename, outPath, expectedPath)
				}

				// Verify path containment
				rel, err := filepath.Rel(absTmpDir, outPath)
				if err != nil || rel == ".." || filepath.IsAbs(rel) {
					t.Errorf("Target path %q escaped downloads dir %q", outPath, absTmpDir)
				}
			}
		})
	}
}

func TestReceiveSanitizesTraversalAndIsolatesFile(t *testing.T) {
	// Change directory to temporary directory so downloads/ is created locally and safely
	testWorkDir, err := os.MkdirTemp("", "test_receive_workdir_*")
	if err != nil {
		t.Fatalf("Failed to create workdir: %v", err)
	}
	defer os.RemoveAll(testWorkDir)

	origWd, _ := os.Getwd()
	if err := os.Chdir(testWorkDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}
	defer os.Chdir(origWd)

	clientConn, serverConn := net.Pipe()
	clientStream := &mockStream{netConn: clientConn}
	serverStream := &mockStream{netConn: serverConn}

	payload := []byte("Hello, secure P2P file transfer!")
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "../../../escaped_payload.txt",
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- Receive(serverStream)
	}()

	// Write header and payload from client
	if err := hdr.WriteTo(clientStream); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}
	if _, err := clientStream.Write(payload); err != nil {
		t.Fatalf("Failed to write payload: %v", err)
	}
	clientStream.Close()

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("Receive failed unexpectedly: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receive timed out")
	}

	// Verify file was written to downloads/escaped_payload.txt
	expectedFilePath := filepath.Join(testWorkDir, "downloads", "escaped_payload.txt")
	content, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("Expected received file at %s, got error: %v", expectedFilePath, err)
	}
	if !bytes.Equal(content, payload) {
		t.Fatalf("Received content mismatch: got %q, want %q", string(content), string(payload))
	}

	// Verify that NO file was created at ../../../escaped_payload.txt outside downloads
	escapedOutsidePath := filepath.Join(testWorkDir, "escaped_payload.txt")
	if _, err := os.Stat(escapedOutsidePath); !os.IsNotExist(err) {
		t.Fatalf("Path traversal attack succeeded! File created outside downloads at %s", escapedOutsidePath)
	}
}

func TestReceiveRejectsNullByteHeader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	clientStream := &mockStream{netConn: clientConn}
	serverStream := &mockStream{netConn: serverConn}

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "malicious\x00.exe",
		FileSize: 10,
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- Receive(serverStream)
	}()

	_ = hdr.WriteTo(clientStream)
	clientStream.Close()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Expected Receive to fail on null byte filename header, but got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receive timed out")
	}
}
