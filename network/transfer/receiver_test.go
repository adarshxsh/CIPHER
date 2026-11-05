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
	remotePeer  peer.ID
	remoteMaddr multiaddr.Multiaddr
}

func (c *mockConn) RemotePeer() peer.ID {
	return c.remotePeer
}

func (c *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	return c.remoteMaddr
}

type mockStream struct {
	network.Stream
	reader io.Reader
	conn   network.Conn
	closed bool
	reset  bool
}

func (s *mockStream) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *mockStream) Close() error {
	s.closed = true
	return nil
}

func (s *mockStream) Reset() error {
	s.reset = true
	return nil
}

func (s *mockStream) Conn() network.Conn {
	return s.conn
}

func newMockStream(r io.Reader) *mockStream {
	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	return &mockStream{
		reader: r,
		conn: &mockConn{
			remotePeer:  peer.ID("test-peer"),
			remoteMaddr: maddr,
		},
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	ms := newMockStream(bytes.NewReader([]byte{})) // empty stream

	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on empty header read, got nil")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on header read error")
	}
	if ms.closed {
		t.Errorf("expected stream.Close() NOT to be called on failure")
	}
}

func TestReceive_StreamCopyError(t *testing.T) {
	filename := "test_copy_err.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	var buf bytes.Buffer
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 100,
		Checksum: [32]byte{},
	}
	_ = header.WriteTo(&buf)
	// Do not append any payload bytes, stream EOF early

	ms := newMockStream(&buf)
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on premature EOF, got nil")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on stream copy error")
	}
	if ms.closed {
		t.Errorf("expected stream.Close() NOT to be called on failure")
	}

	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected partial file %s to be deleted on error", outPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	payload := []byte("hello world corrupt payload")
	wrongChecksum := [32]byte{0xde, 0xad, 0xbe, 0xef}

	var buf bytes.Buffer
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: wrongChecksum,
	}
	_ = header.WriteTo(&buf)
	buf.Write(payload)

	ms := newMockStream(&buf)
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch, got nil")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on checksum mismatch")
	}
	if ms.closed {
		t.Errorf("expected stream.Close() NOT to be called on checksum mismatch")
	}

	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected corrupted file %s to be deleted on checksum mismatch", outPath)
	}
}

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	payload := []byte("hello world successful transfer payload")
	checksum := sha256.Sum256(payload)

	var buf bytes.Buffer
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}
	_ = header.WriteTo(&buf)
	buf.Write(payload)

	ms := newMockStream(&buf)
	err := Receive(ms)
	if err != nil {
		t.Fatalf("expected successful receive, got error: %v", err)
	}

	if ms.reset {
		t.Errorf("expected stream.Reset() NOT to be called on success")
	}
	if !ms.closed {
		t.Errorf("expected stream.Close() to be called on success")
	}

	receivedContent, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("expected downloaded file to exist, failed to read: %v", err)
	}

	if !bytes.Equal(receivedContent, payload) {
		t.Errorf("downloaded content mismatch: expected %s, got %s", payload, receivedContent)
	}
}
