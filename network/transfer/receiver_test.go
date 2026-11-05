package transfer

import (
	"bytes"
	"crypto/sha256"
	"errors"
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

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	network.Stream
	reader       io.Reader
	writer       io.Writer
	closedCalled bool
	resetCalled  bool
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	if m.writer != nil {
		return m.writer.Write(p)
	}
	return len(p), nil
}

func (m *mockStream) Close() error {
	m.closedCalled = true
	return nil
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Protocol() libp2p_protocol.ID {
	return libp2p_protocol.ID("/cipher/transfer/1.0.0")
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

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	fileData := []byte("Hello, atomic transfer staging test!")
	hasher := sha256.New()
	hasher.Write(fileData)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(fileData)),
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(fileData)

	stream := &mockStream{reader: buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	// Cleanup any previous test artifacts
	os.Remove(outPath)
	os.Remove(tmpPath)

	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got error: %v", err)
	}

	if !stream.closedCalled {
		t.Errorf("expected stream.Close() to be called on success")
	}
	if stream.resetCalled {
		t.Errorf("expected stream.Reset() NOT to be called on success")
	}

	// Verify destination file exists and contents match
	readBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if !bytes.Equal(readBytes, fileData) {
		t.Errorf("expected content %q, got %q", fileData, readBytes)
	}

	// Verify staging .tmp file does not exist
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("expected temporary staging file %s to be removed or renamed", tmpPath)
	}

	// Clean up
	os.Remove(outPath)
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	fileData := []byte("Some test data that will fail checksum verification")
	var wrongChecksum [32]byte
	wrongChecksum[0] = 0xFF // invalid checksum

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(fileData)),
		Checksum: wrongChecksum,
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(fileData)

	stream := &mockStream{reader: buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch, got nil")
	}

	if !stream.resetCalled {
		t.Errorf("expected stream.Reset() to be called on checksum mismatch")
	}

	// Verify neither outPath nor tmpPath exists
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("destination file %s should not exist after failed transfer", outPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should be removed after failed transfer", tmpPath)
	}
}

func TestReceive_SizeMismatch(t *testing.T) {
	filename := "test_size_mismatch.txt"
	fileData := []byte("Truncated")
	hasher := sha256.New()
	hasher.Write(fileData)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(1000), // Larger than actual data
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(fileData)

	stream := &mockStream{reader: buf}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on size mismatch, got nil")
	}

	if !stream.resetCalled {
		t.Errorf("expected stream.Reset() to be called on size mismatch")
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should be removed after size mismatch", tmpPath)
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	// Empty reader will fail header read immediately
	buf := new(bytes.Buffer)
	stream := &mockStream{reader: buf}

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on header read failure, got nil")
	}

	if !stream.resetCalled {
		t.Errorf("expected stream.Reset() to be called on header read failure")
	}
}

type errReader struct {
	headerBytes []byte
	readBytes   int
	err         error
}

func (er *errReader) Read(p []byte) (n int, err error) {
	if er.readBytes < len(er.headerBytes) {
		n = copy(p, er.headerBytes[er.readBytes:])
		er.readBytes += n
		return n, nil
	}
	return 0, er.err
}

func TestReceive_IOErrorDuringData(t *testing.T) {
	filename := "test_io_error.txt"
	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(100),
		Checksum: [32]byte{},
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	reader := &errReader{
		headerBytes: buf.Bytes(),
		err:         errors.New("simulated network connection drop"),
	}

	stream := &mockStream{reader: reader}

	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	tmpPath := outPath + ".tmp"

	os.Remove(outPath)
	os.Remove(tmpPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on IO error during data receive, got nil")
	}

	if !stream.resetCalled {
		t.Errorf("expected stream.Reset() to be called on IO error")
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("staging file %s should be removed after IO error", tmpPath)
	}
}
