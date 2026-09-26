package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	p2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

type mockStream struct {
	r           io.Reader
	closed      bool
	resetCalled bool
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	if m.r == nil {
		return 0, io.EOF
	}
	return m.r.Read(p)
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (m *mockStream) Close() error {
	m.closed = true
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

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockStream) ID() string {
	return "mock-stream-1"
}

func (m *mockStream) Protocol() p2p_protocol.ID {
	return p2p_protocol.ID("/cipher/filetransfer/1.0.0")
}

func (m *mockStream) SetProtocol(p p2p_protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Conn() network.Conn {
	return nil
}

func (m *mockStream) Scope() network.StreamScope {
	return nil
}

var _ network.Stream = (*mockStream)(nil)

func createHeaderBytes(filename string, payload []byte, checksumOverride *[32]byte) []byte {
	var checksum [32]byte
	if checksumOverride != nil {
		checksum = *checksumOverride
	} else {
		checksum = sha256.Sum256(payload)
	}

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	var buf bytes.Buffer
	_ = hdr.WriteTo(&buf)
	return buf.Bytes()
}

func TestReceive_Success(t *testing.T) {
	defer os.RemoveAll("downloads")

	filename := fmt.Sprintf("test_success_%d.dat", time.Now().UnixNano())
	payload := []byte("hello world file transfer content success")
	hdrBytes := createHeaderBytes(filename, payload, nil)

	var streamBuf bytes.Buffer
	streamBuf.Write(hdrBytes)
	streamBuf.Write(payload)

	s := &mockStream{r: &streamBuf}

	err := Receive(s)
	if err != nil {
		t.Fatalf("Expected Receive to succeed, got: %v", err)
	}

	if !s.closed {
		t.Errorf("Expected stream to be closed on success")
	}
	if s.resetCalled {
		t.Errorf("Expected s.Reset() NOT to be called on success")
	}

	outPath := filepath.Join("downloads", filename)
	content, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Expected output file to exist, got err: %v", err)
	}
	if !bytes.Equal(content, payload) {
		t.Errorf("File content mismatch: expected %q, got %q", payload, content)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	defer os.RemoveAll("downloads")

	filename := fmt.Sprintf("test_checksum_%d.dat", time.Now().UnixNano())
	payload := []byte("corrupted payload or bad checksum")
	badChecksum := [32]byte{1, 2, 3, 4}
	hdrBytes := createHeaderBytes(filename, payload, &badChecksum)

	var streamBuf bytes.Buffer
	streamBuf.Write(hdrBytes)
	streamBuf.Write(payload)

	s := &mockStream{r: &streamBuf}

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to return error on checksum mismatch, got nil")
	}

	if !s.resetCalled {
		t.Errorf("Expected s.Reset() to be called on checksum mismatch")
	}

	outPath := filepath.Join("downloads", filename)
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("Expected partial file %s to be removed on checksum mismatch", outPath)
	}
}

func TestReceive_InterruptedTransfer(t *testing.T) {
	defer os.RemoveAll("downloads")

	filename := fmt.Sprintf("test_interrupted_%d.dat", time.Now().UnixNano())
	fullPayload := []byte("this is a full payload that will be truncated midway")
	hdrBytes := createHeaderBytes(filename, fullPayload, nil)

	truncatedPayload := fullPayload[:10]

	var streamBuf bytes.Buffer
	streamBuf.Write(hdrBytes)
	streamBuf.Write(truncatedPayload)

	s := &mockStream{r: &streamBuf}

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to return error on interrupted transfer, got nil")
	}

	if !s.resetCalled {
		t.Errorf("Expected s.Reset() to be called on interrupted transfer")
	}

	outPath := filepath.Join("downloads", filename)
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("Expected partial file %s to be removed on interrupted transfer", outPath)
	}
}

func TestReceive_HeaderReadFailure(t *testing.T) {
	defer os.RemoveAll("downloads")

	var streamBuf bytes.Buffer
	s := &mockStream{r: &streamBuf}

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to return error on empty header, got nil")
	}

	if !s.resetCalled {
		t.Errorf("Expected s.Reset() to be called on header read failure")
	}
}

func TestReceive_UnsupportedProtocolVersion(t *testing.T) {
	defer os.RemoveAll("downloads")

	hdr := &Header{
		Version:  99,
		Type:     MsgTypeFileTransfer,
		Filename: "test.dat",
		FileSize: 10,
		Checksum: [32]byte{},
	}
	var streamBuf bytes.Buffer
	_ = hdr.WriteTo(&streamBuf)

	s := &mockStream{r: &streamBuf}

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected error for unsupported protocol version")
	}

	if !s.resetCalled {
		t.Errorf("Expected s.Reset() to be called on unsupported protocol version")
	}
}
