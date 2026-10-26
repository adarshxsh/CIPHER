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
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

type testStream struct {
	reader       io.Reader
	writer       io.Writer
	resetCalled  bool
	closedCalled bool
}

func (s *testStream) Read(p []byte) (n int, err error) {
	if s.reader != nil {
		return s.reader.Read(p)
	}
	return 0, io.EOF
}

func (s *testStream) Write(p []byte) (n int, err error) {
	if s.writer != nil {
		return s.writer.Write(p)
	}
	return len(p), nil
}

func (s *testStream) Close() error {
	s.closedCalled = true
	return nil
}

func (s *testStream) CloseWrite() error { return nil }
func (s *testStream) CloseRead() error  { return nil }

func (s *testStream) Reset() error {
	s.resetCalled = true
	return nil
}

func (s *testStream) ResetWithError(code network.StreamErrorCode) error {
	s.resetCalled = true
	return nil
}

func (s *testStream) SetDeadline(t time.Time) error      { return nil }
func (s *testStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *testStream) SetWriteDeadline(t time.Time) error { return nil }

func (s *testStream) ID() string                              { return "test-stream" }
func (s *testStream) Protocol() libp2p_protocol.ID            { return "/cipher/transfer/1.0.0" }
func (s *testStream) SetProtocol(p libp2p_protocol.ID) error { return nil }
func (s *testStream) Scope() network.StreamScope { return &network.NullScope{} }
func (s *testStream) Stat() network.Stats                     { return network.Stats{} }
func (s *testStream) Conn() network.Conn                      { return nil }

func buildValidHeaderBytes(filename string, payload []byte) []byte {
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

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

func TestReceive_NilStream(t *testing.T) {
	err := Receive(nil)
	if err == nil {
		t.Fatalf("expected error when stream is nil, got nil")
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	ts := &testStream{
		reader: bytes.NewReader([]byte{1, 2}), // Truncated header
	}

	err := Receive(ts)
	if err == nil {
		t.Fatalf("expected error on truncated header read")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on header read failure")
	}
}

func TestReceive_UnsupportedProtocolVersion(t *testing.T) {
	buf := buildValidHeaderBytes("test.txt", []byte("hello"))
	buf[0] = 99 // Invalid version

	ts := &testStream{
		reader: bytes.NewReader(buf),
	}

	err := Receive(ts)
	if err == nil {
		t.Fatalf("expected error for unsupported protocol version")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on unsupported version")
	}
}

func TestReceive_InvalidFilename(t *testing.T) {
	// Header with invalid empty filename
	buf := buildValidHeaderBytes("..", []byte("hello"))
	ts := &testStream{
		reader: bytes.NewReader(buf),
	}

	err := Receive(ts)
	if err == nil {
		t.Fatalf("expected error for invalid filename")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on invalid filename")
	}
}

func TestReceive_PartialTransferDataError(t *testing.T) {
	payload := []byte("this is a test payload for partial transfer failure")
	hdrBytes := buildValidHeaderBytes("partial_test.txt", payload)

	// Combine header + partial payload (only 5 bytes of payload)
	streamData := append(hdrBytes, payload[:5]...)

	ts := &testStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(ts)
	if err == nil {
		t.Fatalf("expected error on partial transfer EOF/mismatch")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on transfer error")
	}

	// Verify partial file was removed from downloads directory
	filePath := filepath.Join("downloads", "partial_test.txt")
	if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
		t.Fatalf("expected partial file %s to be deleted, but it still exists", filePath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	payload := []byte("correct payload")
	hdrBytes := buildValidHeaderBytes("checksum_test.txt", payload)

	corruptPayload := []byte("corrupt payload")
	streamData := append(hdrBytes, corruptPayload...)

	ts := &testStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(ts)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on checksum mismatch")
	}

	// Verify corrupted file was deleted
	filePath := filepath.Join("downloads", "checksum_test.txt")
	if _, statErr := os.Stat(filePath); !os.IsNotExist(statErr) {
		t.Fatalf("expected file %s to be deleted on checksum mismatch, but it exists", filePath)
	}
}

func TestReceive_Success(t *testing.T) {
	payload := []byte("successful transfer data content")
	filename := "success_test.txt"
	hdrBytes := buildValidHeaderBytes(filename, payload)

	streamData := append(hdrBytes, payload...)

	ts := &testStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(ts)
	if err != nil {
		t.Fatalf("unexpected error on valid transfer: %v", err)
	}
	if ts.resetCalled {
		t.Fatalf("stream.Reset() should not be called on successful transfer")
	}
	if !ts.closedCalled {
		t.Fatalf("stream.Close() should be called on successful transfer")
	}

	// Verify file was written properly
	filePath := filepath.Join("downloads", filename)
	defer os.Remove(filePath)

	readData, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if !bytes.Equal(readData, payload) {
		t.Fatalf("output file content mismatch: expected %q, got %q", payload, readData)
	}
}
