package transfer

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
	reader io.Reader
	closed bool
	reset  bool
	conn   *mockConn
}

func newMockStream(r io.Reader) *mockStream {
	return &mockStream{
		reader: r,
		conn:   &mockConn{},
	}
}

func (s *mockStream) Read(p []byte) (n int, err error) {
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

type errReader struct {
	headerBuf *bytes.Buffer
	readCount int
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.headerBuf.Len() > 0 {
		return e.headerBuf.Read(p)
	}
	return 0, errors.New("injected network error")
}

func createTransferBuffer(filename string, data []byte, corruptChecksum bool) *bytes.Buffer {
	buf := new(bytes.Buffer)
	hasher := sha256.New()
	hasher.Write(data)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	if corruptChecksum {
		checksum[0] ^= 0xff
	}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(data)),
		Checksum: checksum,
	}
	_ = header.WriteTo(buf)
	buf.Write(data)
	return buf
}

func TestReceive_Success(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	filename := "test_success.txt"
	data := []byte("Hello, libp2p file transfer!")
	buf := createTransferBuffer(filename, data, false)

	stream := newMockStream(buf)
	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !stream.closed {
		t.Errorf("expected stream to be closed")
	}
	if stream.reset {
		t.Errorf("expected stream reset to be false")
	}

	outPath := filepath.Join("downloads", filename)
	content, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	if !bytes.Equal(content, data) {
		t.Errorf("expected content %q, got %q", data, content)
	}
}

func TestReceive_FilenameSanitization(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	filename := "../../traversal_test.txt"
	data := []byte("sanitized file content")
	buf := createTransferBuffer(filename, data, false)

	stream := newMockStream(buf)
	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify file is saved in downloads/ as traversal_test.txt
	expectedPath := filepath.Join("downloads", "traversal_test.txt")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("expected file at %s, but it was not found", expectedPath)
	}

	// Verify file does not exist outside downloads/
	if _, err := os.Stat("traversal_test.txt"); !os.IsNotExist(err) {
		t.Errorf("file should not exist outside downloads directory")
		_ = os.Remove("traversal_test.txt")
	}
}

func TestReceive_HeaderReadFailure(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	// Truncated header data
	buf := bytes.NewBuffer([]byte{1, 1})
	stream := newMockStream(buf)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on truncated header, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be true on header read error")
	}
}

func TestReceive_DataReadFailure(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	filename := "test_data_fail.txt"
	data := []byte("some payload data")

	// Create header buffer only
	hdrBuf := new(bytes.Buffer)
	hasher := sha256.New()
	hasher.Write(data)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(data)),
		Checksum: checksum,
	}
	_ = header.WriteTo(hdrBuf)

	reader := &errReader{headerBuf: hdrBuf}
	stream := newMockStream(reader)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on data read failure, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be true on data read failure")
	}

	outPath := filepath.Join("downloads", filename)
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected partial file %s to be removed, but it exists", outPath)
	}
}

func TestReceive_SizeMismatch(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	filename := "test_size_mismatch.txt"
	data := []byte("short payload")

	hdrBuf := new(bytes.Buffer)
	var checksum [32]byte

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 1000, // Header claims 1000 bytes, but we only supply short payload
		Checksum: checksum,
	}
	_ = header.WriteTo(hdrBuf)
	hdrBuf.Write(data)

	stream := newMockStream(hdrBuf)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on size mismatch, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be true on size mismatch")
	}

	outPath := filepath.Join("downloads", filename)
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected partial file %s to be removed, but it exists", outPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	_ = os.RemoveAll("downloads")
	defer os.RemoveAll("downloads")

	filename := "test_checksum_mismatch.txt"
	data := []byte("corrupted payload test")
	buf := createTransferBuffer(filename, data, true) // corruptChecksum = true

	stream := newMockStream(buf)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch, got nil")
	}

	if !stream.reset {
		t.Errorf("expected stream reset to be true on checksum mismatch")
	}

	outPath := filepath.Join("downloads", filename)
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected corrupted file %s to be removed, but it exists", outPath)
	}
}
