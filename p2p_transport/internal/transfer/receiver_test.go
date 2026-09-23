package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type mockStream struct {
	reader io.Reader
	writer bytes.Buffer

	resetCalled bool
	closeCalled bool
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	if m.reader == nil {
		return 0, io.EOF
	}
	return m.reader.Read(p)
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	return m.writer.Write(p)
}

func (m *mockStream) Close() error {
	m.closeCalled = true
	return nil
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) ResetWithError(network.StreamErrorCode) error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) SetDeadline(time.Time) error {
	return nil
}

func (m *mockStream) SetReadDeadline(time.Time) error {
	return nil
}

func (m *mockStream) SetWriteDeadline(time.Time) error {
	return nil
}

func (m *mockStream) ID() string {
	return "mock-stream-id"
}

func (m *mockStream) Protocol() protocol.ID {
	return "/cipher/filetransfer/1.0.0"
}

func (m *mockStream) SetProtocol(protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Conn() network.Conn {
	return nil
}

func (m *mockStream) Scope() network.StreamScope {
	return &network.NullScope{}
}

func TestReceive_HeaderReadFailure(t *testing.T) {
	ms := &mockStream{
		reader: bytes.NewReader([]byte{}), // empty stream
	}

	err := Receive(ms)
	if err == nil {
		t.Fatal("expected error on header read failure, got nil")
	}

	if !ms.resetCalled {
		t.Error("expected stream.Reset() to be called on header read failure")
	}
	if ms.closeCalled {
		t.Error("expected stream.Close() NOT to be called on failure")
	}
}

func TestReceive_InvalidProtocolVersion(t *testing.T) {
	buf := new(bytes.Buffer)
	header := &Header{
		Version:  99,
		Type:     MsgTypeFileTransfer,
		Filename: "test.txt",
		FileSize: 10,
	}
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to encode header: %v", err)
	}

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatal("expected error on invalid protocol version, got nil")
	}

	if !ms.resetCalled {
		t.Error("expected stream.Reset() to be called on invalid protocol version")
	}
	if ms.closeCalled {
		t.Error("expected stream.Close() NOT to be called on failure")
	}
}

func TestReceive_TruncatedData(t *testing.T) {
	filename := "truncated_test.txt"
	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	// Cleanup before/after test
	os.Remove(outPath)
	os.Remove(tmpPath)
	defer func() {
		os.Remove(outPath)
		os.Remove(tmpPath)
	}()

	payload := []byte("hello world long payload")
	sum := sha256.Sum256(payload)

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: sum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to encode header: %v", err)
	}
	// Write only part of the payload
	buf.Write(payload[:5])

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatal("expected error on truncated stream copy, got nil")
	}

	if !ms.resetCalled {
		t.Error("expected stream.Reset() to be called on truncated payload")
	}
	if ms.closeCalled {
		t.Error("expected stream.Close() NOT to be called on failure")
	}

	// Verify temporary staging file is deleted
	if _, statErr := os.Stat(tmpPath); !os.IsNotExist(statErr) {
		t.Errorf("temporary staging file %s still exists after failure", tmpPath)
	}
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("output file %s exists despite failed transfer", outPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "checksum_mismatch_test.txt"
	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)
	defer func() {
		os.Remove(outPath)
		os.Remove(tmpPath)
	}()

	payload := []byte("corrupt file payload")
	wrongChecksum := sha256.Sum256([]byte("different data"))

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: wrongChecksum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(payload)

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatal("expected error on checksum mismatch, got nil")
	}

	if !ms.resetCalled {
		t.Error("expected stream.Reset() to be called on checksum mismatch")
	}
	if ms.closeCalled {
		t.Error("expected stream.Close() NOT to be called on failure")
	}

	// Verify temporary file removed and output file not promoted
	if _, statErr := os.Stat(tmpPath); !os.IsNotExist(statErr) {
		t.Errorf("temporary staging file %s still exists after checksum failure", tmpPath)
	}
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("output file %s exists after checksum mismatch", outPath)
	}
}

func TestReceive_Success(t *testing.T) {
	filename := "valid_transfer_test.txt"
	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)
	defer func() {
		os.Remove(outPath)
		os.Remove(tmpPath)
	}()

	payload := []byte("perfect file payload content")
	sum := sha256.Sum256(payload)

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: sum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to encode header: %v", err)
	}
	buf.Write(payload)

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got: %v", err)
	}

	if ms.resetCalled {
		t.Error("expected stream.Reset() NOT to be called on success")
	}
	if !ms.closeCalled {
		t.Error("expected stream.Close() to be called gracefully on success")
	}

	// Check staging file deleted and output file exists
	if _, statErr := os.Stat(tmpPath); !os.IsNotExist(statErr) {
		t.Errorf("temporary staging file %s still exists after success", tmpPath)
	}

	readContent, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("failed to read output file %s: %v", outPath, readErr)
	}
	if !bytes.Equal(readContent, payload) {
		t.Errorf("expected file content %q, got %q", payload, readContent)
	}
}

func TestReceive_PathTraversalSanitization(t *testing.T) {
	filename := "../../path_traversal_test.txt"
	sanitizedFilename := "path_traversal_test.txt"
	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, sanitizedFilename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)
	defer func() {
		os.Remove(outPath)
		os.Remove(tmpPath)
	}()

	payload := []byte("path traversal prevention payload")
	sum := sha256.Sum256(payload)

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: sum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to encode header: %v", err)
	}
	buf.Write(payload)

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got: %v", err)
	}

	// Verify file was written to downloadsDir/sanitizedFilename
	readContent, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("failed to read sanitized output file %s: %v", outPath, readErr)
	}
	if !bytes.Equal(readContent, payload) {
		t.Errorf("expected content %q, got %q", payload, readContent)
	}
}
