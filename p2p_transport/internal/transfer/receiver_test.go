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

func (c *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

func (c *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	m, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return m
}

type mockStream struct {
	network.Stream
	r      io.Reader
	closed bool
	reset  bool
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.r.Read(p)
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

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	content := []byte("Hello, this is a test transfer payload!")
	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(content)

	stream := &mockStream{r: &buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"
	defer os.Remove(outPath)
	defer os.Remove(tmpPath)

	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got: %v", err)
	}

	if !stream.closed {
		t.Errorf("expected stream to be closed on success")
	}
	if stream.reset {
		t.Errorf("expected stream not to be reset on success")
	}

	// Verify target file exists and content matches
	savedContent, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("expected output file to exist, got err: %v", err)
	}
	if !bytes.Equal(savedContent, content) {
		t.Errorf("content mismatch: got %s, want %s", string(savedContent), string(content))
	}

	// Verify staging file does not exist
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s still exists", tmpPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_corrupt.txt"
	content := []byte("Hello, this is corrupted payload!")
	var fakeChecksum [32]byte
	copy(fakeChecksum[:], []byte("12345678901234567890123456789012"))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: fakeChecksum,
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(content)

	stream := &mockStream{r: &buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"
	defer os.Remove(outPath)
	defer os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be called on failure")
	}

	// Ensure no partial or target file remains
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("target file %s should not exist after checksum mismatch", outPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should not exist after checksum mismatch", tmpPath)
	}
}

func TestReceive_SizeMismatch(t *testing.T) {
	filename := "test_short.txt"
	content := []byte("Truncated payload")
	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)) + 100, // Header expects 100 more bytes
		Checksum: checksum,
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(content)

	stream := &mockStream{r: &buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"
	defer os.Remove(outPath)
	defer os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected size mismatch error, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be called on failure")
	}

	// Ensure no partial or target file remains
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("target file %s should not exist after size mismatch", outPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should not exist after size mismatch", tmpPath)
	}
}

func TestReceive_HeaderError(t *testing.T) {
	filename := "test_badheader.txt"
	hdr := &Header{
		Version:  99, // Unsupported protocol version
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 10,
		Checksum: [32]byte{},
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	stream := &mockStream{r: &buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"
	defer os.Remove(outPath)
	defer os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected header error, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be called on header error")
	}

	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("target file %s should not exist on header error", outPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should not exist on header error", tmpPath)
	}
}
