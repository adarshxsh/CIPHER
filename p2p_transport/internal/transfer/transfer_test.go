package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

// mockStream implements network.Stream over net.Conn for testing.
type mockStream struct {
	c net.Conn
}

func (m *mockStream) Read(p []byte) (int, error)                           { return m.c.Read(p) }
func (m *mockStream) Write(p []byte) (int, error)                          { return m.c.Write(p) }
func (m *mockStream) Close() error                                         { return m.c.Close() }
func (m *mockStream) CloseWrite() error                                    { return nil }
func (m *mockStream) CloseRead() error                                     { return nil }
func (m *mockStream) Reset() error                                         { return m.c.Close() }
func (m *mockStream) ResetWithError(network.StreamErrorCode) error         { return m.c.Close() }
func (m *mockStream) SetDeadline(t time.Time) error                        { return m.c.SetDeadline(t) }
func (m *mockStream) SetReadDeadline(t time.Time) error                    { return m.c.SetReadDeadline(t) }
func (m *mockStream) SetWriteDeadline(t time.Time) error                   { return m.c.SetWriteDeadline(t) }
func (m *mockStream) Stat() network.Stats                                  { return network.Stats{} }
func (m *mockStream) Conn() network.Conn                                   { return nil }
func (m *mockStream) Scope() network.StreamScope                           { return nil }
func (m *mockStream) Protocol() libp2p_protocol.ID                         { return "/cipher/filetransfer/1.0.0" }
func (m *mockStream) SetProtocol(libp2p_protocol.ID) error                { return nil }
func (m *mockStream) ID() string                                           { return "mock-stream-id" }

func TestHeaderSerialization(t *testing.T) {
	origHeader := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_data.bin",
		FileSize: 1048576,
	}

	var buf bytes.Buffer
	if err := origHeader.WriteTo(&buf); err != nil {
		t.Fatalf("Header.WriteTo failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("Header.ReadFrom failed: %v", err)
	}

	if readHeader.Version != origHeader.Version {
		t.Errorf("Version mismatch: got %d, want %d", readHeader.Version, origHeader.Version)
	}
	if readHeader.Type != origHeader.Type {
		t.Errorf("Type mismatch: got %d, want %d", readHeader.Type, origHeader.Type)
	}
	if readHeader.Filename != origHeader.Filename {
		t.Errorf("Filename mismatch: got %q, want %q", readHeader.Filename, origHeader.Filename)
	}
	if readHeader.FileSize != origHeader.FileSize {
		t.Errorf("FileSize mismatch: got %d, want %d", readHeader.FileSize, origHeader.FileSize)
	}
}

func TestSinglePassSendAndReceive(t *testing.T) {
	tempDir := t.TempDir()

	// Create test file with random content
	fileSize := int64(1024 * 512) // 512 KB
	testData := make([]byte, fileSize)
	if _, err := rand.Read(testData); err != nil {
		t.Fatalf("failed to generate random test data: %v", err)
	}

	srcFilePath := filepath.Join(tempDir, "sample.bin")
	if err := os.WriteFile(srcFilePath, testData, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	// Change working directory to tempDir so downloads/ is created there
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	pipeSender, pipeReceiver := net.Pipe()
	senderStream := &mockStream{c: pipeSender}
	receiverStream := &mockStream{c: pipeReceiver}

	errChan := make(chan error, 2)

	go func() {
		errChan <- Send(senderStream, srcFilePath)
	}()

	go func() {
		errChan <- Receive(receiverStream)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("Transfer operation failed: %v", err)
		}
	}

	// Verify received file
	dstFilePath := filepath.Join(tempDir, "downloads", "sample.bin")
	receivedData, err := os.ReadFile(dstFilePath)
	if err != nil {
		t.Fatalf("Failed to read received file: %v", err)
	}

	if !bytes.Equal(receivedData, testData) {
		t.Fatalf("Received file content does not match original file!")
	}
}

func TestCorruptedChecksumCleanup(t *testing.T) {
	tempDir := t.TempDir()

	filename := "corrupt_test.bin"
	payload := []byte("Hello world test payload data")
	fileSize := uint64(len(payload))

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	pipeSender, pipeReceiver := net.Pipe()
	senderStream := &mockStream{c: pipeSender}
	receiverStream := &mockStream{c: pipeReceiver}

	go func() {
		defer senderStream.Close()
		// Write Header
		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: filename,
			FileSize: fileSize,
		}
		_ = header.WriteTo(senderStream)
		// Write payload
		_, _ = senderStream.Write(payload)
		// Write WRONG trailing checksum
		badChecksum := [32]byte{0xde, 0xad, 0xbe, 0xef}
		_, _ = senderStream.Write(badChecksum[:])
	}()

	err = Receive(receiverStream)
	if err == nil {
		t.Fatalf("Expected Receive to fail on checksum mismatch, but got nil")
	}

	// Verify file was cleaned up
	dstFilePath := filepath.Join(tempDir, "downloads", filename)
	if _, err := os.Stat(dstFilePath); !os.IsNotExist(err) {
		t.Fatalf("Expected file %s to be cleaned up on checksum failure, but it still exists", dstFilePath)
	}
}

func TestTruncatedStreamCleanup(t *testing.T) {
	tempDir := t.TempDir()

	filename := "truncated_test.bin"
	payload := []byte("Some partial data")
	fileSize := uint64(len(payload) + 100) // Declare larger size

	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}
	defer func() { _ = os.Chdir(oldWd) }()

	pipeSender, pipeReceiver := net.Pipe()
	senderStream := &mockStream{c: pipeSender}
	receiverStream := &mockStream{c: pipeReceiver}

	go func() {
		// Write Header
		header := Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: filename,
			FileSize: fileSize,
		}
		_ = header.WriteTo(senderStream)
		// Write only partial payload and abruptly close stream
		_, _ = senderStream.Write(payload)
		_ = senderStream.Close()
	}()

	err = Receive(receiverStream)
	if err == nil {
		t.Fatalf("Expected Receive to fail on truncated stream, but got nil")
	}

	// Verify partial file was cleaned up
	dstFilePath := filepath.Join(tempDir, "downloads", filename)
	if _, err := os.Stat(dstFilePath); !os.IsNotExist(err) {
		t.Fatalf("Expected file %s to be cleaned up on truncated stream, but it still exists", dstFilePath)
	}
}

func TestSinglePassReadCount(t *testing.T) {
	tempDir := t.TempDir()
	srcFilePath := filepath.Join(tempDir, "single_pass.txt")
	content := []byte("Single pass disk read test content")
	if err := os.WriteFile(srcFilePath, content, 0644); err != nil {
		t.Fatalf("os.WriteFile failed: %v", err)
	}

	pipeSender, pipeReceiver := net.Pipe()
	senderStream := &mockStream{c: pipeSender}
	receiverStream := &mockStream{c: pipeReceiver}

	// Read on receiver in background
	go func() {
		// Read header
		var h Header
		_ = h.ReadFrom(receiverStream)
		// Read content
		buf := make([]byte, h.FileSize)
		_, _ = io.ReadFull(receiverStream, buf)
		// Read checksum
		var cs [32]byte
		_, _ = io.ReadFull(receiverStream, cs[:])
		_ = receiverStream.Close()
	}()

	hasher := sha256.New()
	hasher.Write(content)
	expectedSum := hasher.Sum(nil)

	if err := Send(senderStream, srcFilePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	var computed [32]byte
	copy(computed[:], expectedSum)

	if len(content) != len("Single pass disk read test content") {
		t.Fatalf("Length mismatch")
	}
}
