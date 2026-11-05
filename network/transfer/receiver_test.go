package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	network.Stream
	reader      io.Reader
	resetCalled bool
	closeCalled bool
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) Close() error {
	m.closeCalled = true
	return nil
}

func createHeaderBuffer(t *testing.T, version byte, msgType byte, filename string, payload []byte, badChecksum bool) []byte {
	t.Helper()
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	if badChecksum {
		checksum[0] ^= 0xFF
	}

	h := Header{
		Version:  version,
		Type:     msgType,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	var buf bytes.Buffer
	if err := h.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	return buf.Bytes()
}

func TestReceiveHeaderReadFailure(t *testing.T) {
	stream := &mockStream{
		reader: bytes.NewReader([]byte{}), // Empty stream
	}

	err := Receive(stream)
	if err == nil {
		t.Fatal("expected error on empty stream header read, got nil")
	}

	if !stream.resetCalled {
		t.Error("expected stream.Reset() to be called on header read failure")
	}
	if stream.closeCalled {
		t.Error("stream.Close() should not be called when Reset() is expected")
	}
}

func TestReceiveUnsupportedHeader(t *testing.T) {
	headerBytes := createHeaderBuffer(t, 2, MsgTypeFileTransfer, "test.txt", []byte("hello"), false)
	stream := &mockStream{
		reader: bytes.NewReader(headerBytes),
	}

	err := Receive(stream)
	if err == nil {
		t.Fatal("expected error on unsupported version, got nil")
	}

	if !stream.resetCalled {
		t.Error("expected stream.Reset() to be called on unsupported protocol version")
	}
}

func TestReceiveDataCopyError(t *testing.T) {
	filename := "copy_err_test.txt"
	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)

	payload := []byte("hello world")
	headerBytes := createHeaderBuffer(t, ProtocolVersion1, MsgTypeFileTransfer, filename, payload, false)

	// Stream header complete, but data truncated
	truncatedData := append(headerBytes, payload[:3]...)
	stream := &mockStream{
		reader: bytes.NewReader(truncatedData),
	}

	err := Receive(stream)
	if err == nil {
		t.Fatal("expected error on truncated data copy, got nil")
	}

	if !stream.resetCalled {
		t.Error("expected stream.Reset() to be called on data copy error")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file %s to be purged after copy error, but it exists", outPath)
	}
}

func TestReceiveChecksumMismatch(t *testing.T) {
	filename := "checksum_err_test.txt"
	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)

	payload := []byte("important content")
	headerBytes := createHeaderBuffer(t, ProtocolVersion1, MsgTypeFileTransfer, filename, payload, true) // bad checksum

	fullData := append(headerBytes, payload...)
	stream := &mockStream{
		reader: bytes.NewReader(fullData),
	}

	err := Receive(stream)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}

	if !stream.resetCalled {
		t.Error("expected stream.Reset() to be called on checksum mismatch")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file %s to be purged after checksum failure, but it exists", outPath)
	}
}

func TestReceiveSuccess(t *testing.T) {
	filename := "success_test.txt"
	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)
	defer os.Remove(outPath)

	payload := []byte("perfect file payload content")
	headerBytes := createHeaderBuffer(t, ProtocolVersion1, MsgTypeFileTransfer, filename, payload, false)

	fullData := append(headerBytes, payload...)
	stream := &mockStream{
		reader: bytes.NewReader(fullData),
	}

	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected successful Receive, got: %v", err)
	}

	if !stream.closeCalled {
		t.Error("expected stream.Close() to be called on successful transfer")
	}
	if stream.resetCalled {
		t.Error("stream.Reset() should not be called on successful transfer")
	}

	content, readErr := os.ReadFile(outPath)
	if readErr != nil {
		t.Fatalf("failed to read output file: %v", readErr)
	}
	if !bytes.Equal(content, payload) {
		t.Errorf("expected file content %q, got %q", payload, content)
	}
}
