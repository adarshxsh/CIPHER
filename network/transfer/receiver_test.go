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
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

type mockConn struct {
	network.Conn
}

func (c *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

func (c *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	m, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return m
}

type mockStream struct {
	network.Stream
	reader      io.Reader
	closeCalled bool
	resetCalled bool
	conn        network.Conn
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return 0, nil
}

func (m *mockStream) Close() error {
	m.closeCalled = true
	return nil
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) Conn() network.Conn {
	if m.conn != nil {
		return m.conn
	}
	return &mockConn{}
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) ID() string {
	return "test-stream"
}

func (m *mockStream) Protocol() libp2p_protocol.ID {
	return libp2p_protocol.ID("/cipher/transfer/1.0.0")
}

func (m *mockStream) SetProtocol(libp2p_protocol.ID) error {
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

func createHeaderBytes(filename string, fileSize uint64, checksum [32]byte) []byte {
	hdr := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: fileSize,
		Checksum: checksum,
	}
	var buf bytes.Buffer
	_ = hdr.WriteTo(&buf)
	return buf.Bytes()
}

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	content := []byte("hello p2p transfer testing content")
	checksum := sha256.Sum256(content)

	headerBytes := createHeaderBytes(filename, uint64(len(content)), checksum)
	streamData := append(headerBytes, content...)

	s := &mockStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(s)
	if err != nil {
		t.Fatalf("expected nil error on successful transfer, got: %v", err)
	}

	if !s.closeCalled {
		t.Errorf("expected s.Close() to be called on success")
	}
	if s.resetCalled {
		t.Errorf("expected s.Reset() NOT to be called on success")
	}

	gotData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("expected output file to exist at %s: %v", outPath, err)
	}
	if !bytes.Equal(gotData, content) {
		t.Errorf("file content mismatch: expected %s, got %s", content, gotData)
	}
}

func TestReceive_HeaderError(t *testing.T) {
	s := &mockStream{
		reader: bytes.NewReader([]byte("short junk")),
	}

	err := Receive(s)
	if err == nil {
		t.Fatalf("expected error on bad header")
	}

	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on header failure")
	}
	if s.closeCalled {
		t.Errorf("expected s.Close() NOT to be called on header failure")
	}
}

func TestReceive_TruncatedDataError(t *testing.T) {
	filename := "test_truncated.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	checksum := sha256.Sum256([]byte("full content"))
	// Header says 1000 bytes, but stream ends early
	headerBytes := createHeaderBytes(filename, 1000, checksum)
	streamData := append(headerBytes, []byte("short")...)

	s := &mockStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(s)
	if err == nil {
		t.Fatalf("expected error on truncated stream data")
	}

	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on transfer failure")
	}
	if s.closeCalled {
		t.Errorf("expected s.Close() NOT to be called on transfer failure")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file %s to be deleted after transfer error", outPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	content := []byte("real payload data")
	wrongChecksum := sha256.Sum256([]byte("different payload data"))

	headerBytes := createHeaderBytes(filename, uint64(len(content)), wrongChecksum)
	streamData := append(headerBytes, content...)

	s := &mockStream{
		reader: bytes.NewReader(streamData),
	}

	err := Receive(s)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch")
	}

	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on checksum mismatch")
	}
	if s.closeCalled {
		t.Errorf("expected s.Close() NOT to be called on checksum mismatch")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected file %s to be deleted after checksum mismatch", outPath)
	}
}
