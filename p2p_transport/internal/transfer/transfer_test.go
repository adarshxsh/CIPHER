package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

// mockStream wraps net.Conn to fulfill libp2p network.Stream
type mockStream struct {
	c net.Conn
}

func (m *mockStream) Read(p []byte) (int, error)                              { return m.c.Read(p) }
func (m *mockStream) Write(p []byte) (int, error)                             { return m.c.Write(p) }
func (m *mockStream) Close() error                                            { return m.c.Close() }
func (m *mockStream) CloseRead() error                                        { return nil }
func (m *mockStream) CloseWrite() error                                       { return nil }
func (m *mockStream) Reset() error                                            { return m.c.Close() }
func (m *mockStream) ResetWithError(errCode network.StreamErrorCode) error   { return m.c.Close() }
func (m *mockStream) SetDeadline(t time.Time) error                           { return m.c.SetDeadline(t) }
func (m *mockStream) SetReadDeadline(t time.Time) error                      { return m.c.SetReadDeadline(t) }
func (m *mockStream) SetWriteDeadline(t time.Time) error                     { return m.c.SetWriteDeadline(t) }
func (m *mockStream) ID() string                                              { return "mock-stream" }
func (m *mockStream) Protocol() libp2p_protocol.ID                            { return "/file-transfer/1.0.0" }
func (m *mockStream) SetProtocol(id libp2p_protocol.ID) error                { return nil }
func (m *mockStream) Stat() network.Stats                                     { return network.Stats{} }
func (m *mockStream) Conn() network.Conn                                      { return nil }
func (m *mockStream) Scope() network.StreamScope                              { return nil }

func newMockStreamPair() (network.Stream, network.Stream) {
	c1, c2 := net.Pipe()
	return &mockStream{c: c1}, &mockStream{c: c2}
}

func TestHeaderSerialization(t *testing.T) {
	// Test ProtocolVersion1 Header
	h1 := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_v1.txt",
		FileSize: 1024,
		Checksum: sha256.Sum256([]byte("hello v1")),
	}

	buf1 := new(bytes.Buffer)
	if err := h1.WriteTo(buf1); err != nil {
		t.Fatalf("Failed to write V1 header: %v", err)
	}

	h1Read := &Header{}
	if err := h1Read.ReadFrom(buf1); err != nil {
		t.Fatalf("Failed to read V1 header: %v", err)
	}

	if h1Read.Version != ProtocolVersion1 || h1Read.Filename != h1.Filename || h1Read.FileSize != h1.FileSize || h1Read.Checksum != h1.Checksum {
		t.Fatalf("V1 header mismatch: %+v vs %+v", h1, h1Read)
	}

	// Test ProtocolVersion2 Header
	h2 := &Header{
		Version:  ProtocolVersion2,
		Type:     MsgTypeFileTransfer,
		Filename: "test_v2.txt",
		FileSize: 2048,
	}

	buf2 := new(bytes.Buffer)
	if err := h2.WriteTo(buf2); err != nil {
		t.Fatalf("Failed to write V2 header: %v", err)
	}

	h2Read := &Header{}
	if err := h2Read.ReadFrom(buf2); err != nil {
		t.Fatalf("Failed to read V2 header: %v", err)
	}

	if h2Read.Version != ProtocolVersion2 || h2Read.Filename != h2.Filename || h2Read.FileSize != h2.FileSize {
		t.Fatalf("V2 header mismatch: %+v vs %+v", h2, h2Read)
	}
}

func TestSinglePassTransfer_SmallAndLargeFiles(t *testing.T) {
	testSizes := []struct {
		name string
		size int
	}{
		{"empty_file", 0},
		{"small_file", 100 * 1024},       // 100 KB
		{"large_file", 2 * 1024 * 1024},  // 2 MB
	}

	for _, tc := range testSizes {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			srcPath := filepath.Join(tmpDir, "source.bin")

			content := make([]byte, tc.size)
			if tc.size > 0 {
				if _, err := rand.Read(content); err != nil {
					t.Fatalf("Failed to generate random content: %v", err)
				}
			}
			if err := os.WriteFile(srcPath, content, 0644); err != nil {
				t.Fatalf("Failed to create source file: %v", err)
			}

			// Save working directory and change to tmpDir so downloads/ goes to tmpDir
			origWd, err := os.Getwd()
			if err != nil {
				t.Fatalf("Failed to get current wd: %v", err)
			}
			if err := os.Chdir(tmpDir); err != nil {
				t.Fatalf("Failed to chdir: %v", err)
			}
			defer func() { _ = os.Chdir(origWd) }()

			s1, s2 := newMockStreamPair()

			errCh := make(chan error, 2)

			go func() {
				errCh <- Send(s1, srcPath)
			}()

			go func() {
				errCh <- Receive(s2)
			}()

			for i := 0; i < 2; i++ {
				if err := <-errCh; err != nil {
					t.Fatalf("Transfer failed: %v", err)
				}
			}

			dstPath := filepath.Join("downloads", filepath.Base(srcPath))
			dstContent, err := os.ReadFile(dstPath)
			if err != nil {
				t.Fatalf("Failed to read downloaded file: %v", err)
			}

			if !bytes.Equal(content, dstContent) {
				t.Fatalf("Downloaded content mismatch! Expected size %d, got %d", len(content), len(dstContent))
			}
		})
	}
}

func TestBackwardCompatibility_V1Header(t *testing.T) {
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}
	defer func() { _ = os.Chdir(origWd) }()

	payload := []byte("Legacy ProtocolVersion1 Payload Data")
	checksum := sha256.Sum256(payload)

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "legacy.txt",
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	s1, s2 := newMockStreamPair()

	go func() {
		defer s1.Close()
		_ = header.WriteTo(s1)
		_, _ = s1.Write(payload)
	}()

	if err := Receive(s2); err != nil {
		t.Fatalf("Receive failed for ProtocolVersion1: %v", err)
	}

	dstPath := filepath.Join("downloads", "legacy.txt")
	dstContent, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("Failed to read legacy output file: %v", err)
	}

	if !bytes.Equal(payload, dstContent) {
		t.Fatalf("Content mismatch for legacy V1: expected %s, got %s", payload, dstContent)
	}
}

func TestTrailingChecksumMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}
	defer func() { _ = os.Chdir(origWd) }()

	payload := []byte("Corrupted Trailing Checksum Test")
	corruptedChecksum := sha256.Sum256([]byte("wrong payload"))

	header := &Header{
		Version:  ProtocolVersion2,
		Type:     MsgTypeFileTransfer,
		Filename: "corrupted.txt",
		FileSize: uint64(len(payload)),
	}

	s1, s2 := newMockStreamPair()

	go func() {
		defer s1.Close()
		_ = header.WriteTo(s1)
		_, _ = s1.Write(payload)
		_, _ = s1.Write(corruptedChecksum[:])
	}()

	err = Receive(s2)
	if err == nil {
		t.Fatalf("Expected Receive to fail on trailing checksum mismatch, but it succeeded")
	}
}

func TestInterruptedStream(t *testing.T) {
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}
	defer func() { _ = os.Chdir(origWd) }()

	payload := []byte("Truncated payload stream...")

	header := &Header{
		Version:  ProtocolVersion2,
		Type:     MsgTypeFileTransfer,
		Filename: "truncated.txt",
		FileSize: uint64(len(payload) + 100), // Expecting 100 more bytes
	}

	s1, s2 := newMockStreamPair()

	go func() {
		_ = header.WriteTo(s1)
		_, _ = s1.Write(payload)
		_ = s1.Close() // Close mid-stream
	}()

	err = Receive(s2)
	if err == nil {
		t.Fatalf("Expected Receive to fail on truncated stream, but it succeeded")
	}
}

func TestSenderNoSeekCalls(t *testing.T) {
	// Statically inspect sender.go AST to verify zero Seek calls for full-file hashing or rewinding
	senderPath := "sender.go"
	content, err := os.ReadFile(senderPath)
	if err != nil {
		t.Fatalf("Failed to read sender.go: %v", err)
	}

	if bytes.Contains(content, []byte(".Seek(")) {
		t.Fatalf("sender.go contains Seek call! Full-file rewinding/pre-hashing is not permitted.")
	}
}
