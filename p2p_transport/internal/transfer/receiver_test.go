package transfer_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/transfer"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("mockPeer")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	network.Stream
	readBuf *bytes.Buffer
	readErr error
	closed  bool
	reset   bool
}

func newMockStream(data []byte) *mockStream {
	return &mockStream{
		readBuf: bytes.NewBuffer(data),
	}
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	n, err = m.readBuf.Read(p)
	if err == io.EOF && m.readErr != nil {
		return n, m.readErr
	}
	return n, err
}

func (m *mockStream) Close() error {
	m.closed = true
	return nil
}

func (m *mockStream) Reset() error {
	m.reset = true
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func buildHeaderBytes(filename string, payload []byte, checksumOverride *[32]byte) []byte {
	var checksum [32]byte
	if checksumOverride != nil {
		checksum = *checksumOverride
	} else {
		hasher := sha256.New()
		hasher.Write(payload)
		copy(checksum[:], hasher.Sum(nil))
	}

	header := transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	var buf bytes.Buffer
	_ = header.WriteTo(&buf)
	return buf.Bytes()
}

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	payload := []byte("Hello, world! Complete file content.")
	headerBytes := buildHeaderBytes(filename, payload, nil)

	fullStreamData := append(headerBytes, payload...)
	s := newMockStream(fullStreamData)

	defer os.RemoveAll("downloads")

	err := transfer.Receive(s)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !s.closed {
		t.Errorf("Expected stream to be closed cleanly")
	}
	if s.reset {
		t.Errorf("Expected stream NOT to be reset")
	}

	outPath := filepath.Join("downloads", filename)
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Expected file to exist on disk, got err: %v", err)
	}

	if !bytes.Equal(data, payload) {
		t.Errorf("Expected file content %q, got %q", payload, data)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_corrupt.txt"
	payload := []byte("Corrupted file data.")
	badChecksum := [32]byte{0xff, 0xee, 0xdd}
	headerBytes := buildHeaderBytes(filename, payload, &badChecksum)

	fullStreamData := append(headerBytes, payload...)
	s := newMockStream(fullStreamData)

	defer os.RemoveAll("downloads")

	err := transfer.Receive(s)
	if err == nil {
		t.Fatalf("Expected checksum mismatch error, got nil")
	}

	if !s.reset {
		t.Errorf("Expected stream to be Reset on checksum failure")
	}

	outPath := filepath.Join("downloads", filename)
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("Expected partial file to be removed on checksum failure, but it exists")
	}
}

func TestReceive_InterruptedTransfer(t *testing.T) {
	filename := "test_interrupted.txt"
	payload := []byte("Some partial payload that gets cut off")
	headerBytes := buildHeaderBytes(filename, payload, nil)

	// Send header + part of payload, then error
	partialStreamData := append(headerBytes, payload[:10]...)
	s := newMockStream(partialStreamData)
	s.readErr = errors.New("connection reset")

	defer os.RemoveAll("downloads")

	err := transfer.Receive(s)
	if err == nil {
		t.Fatalf("Expected error on interrupted transfer, got nil")
	}

	if !s.reset {
		t.Errorf("Expected stream to be Reset on transfer failure")
	}

	outPath := filepath.Join("downloads", filename)
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("Expected partial file to be removed on transfer failure, but it exists")
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	s := newMockStream([]byte{1}) // truncated header
	s.readErr = errors.New("read error during header")

	defer os.RemoveAll("downloads")

	err := transfer.Receive(s)
	if err == nil {
		t.Fatalf("Expected header read error, got nil")
	}

	if !s.reset {
		t.Errorf("Expected stream to be Reset on header read failure")
	}
}
