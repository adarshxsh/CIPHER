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

type mockStream struct {
	network.Stream
	reader io.Reader
	closed bool
	reset  bool
	conn   *mockConn
}

type mockConn struct {
	network.Conn
	remotePeer peer.ID
	remoteAddr multiaddr.Multiaddr
}

func (mc *mockConn) RemotePeer() peer.ID {
	return mc.remotePeer
}

func (mc *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	if mc.remoteAddr == nil {
		addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
		return addr
	}
	return mc.remoteAddr
}

func (ms *mockStream) Read(p []byte) (n int, err error) {
	if ms.reset {
		return 0, errors.New("stream reset")
	}
	return ms.reader.Read(p)
}

func (ms *mockStream) Close() error {
	ms.closed = true
	return nil
}

func (ms *mockStream) Reset() error {
	ms.reset = true
	return nil
}

func (ms *mockStream) Conn() network.Conn {
	if ms.conn == nil {
		return &mockConn{}
	}
	return ms.conn
}

type faultReader struct {
	headerData []byte
	headerRead int
	failAfter  int
	bytesRead  int
}

func (fr *faultReader) Read(p []byte) (int, error) {
	if fr.headerRead < len(fr.headerData) {
		n := copy(p, fr.headerData[fr.headerRead:])
		fr.headerRead += n
		return n, nil
	}
	if fr.bytesRead >= fr.failAfter {
		return 0, errors.New("simulated network failure")
	}
	n := len(p)
	if fr.bytesRead+n > fr.failAfter {
		n = fr.failAfter - fr.bytesRead
	}
	for i := 0; i < n; i++ {
		p[i] = 'A'
	}
	fr.bytesRead += n
	if fr.bytesRead >= fr.failAfter {
		return n, errors.New("simulated network failure")
	}
	return n, nil
}

func TestReceive_Success(t *testing.T) {
	downloadsDir := "downloads"
	defer os.RemoveAll(downloadsDir)

	payload := []byte("hello world file transfer data")
	sum := sha256.Sum256(payload)

	hdr := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_success.txt",
		FileSize: uint64(len(payload)),
		Checksum: sum,
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(payload)

	ms := &mockStream{reader: &buf}
	err := Receive(ms)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got: %v", err)
	}

	if !ms.closed {
		t.Errorf("expected stream to be closed cleanly")
	}
	if ms.reset {
		t.Errorf("did not expect stream to be reset on success")
	}

	finalPath := filepath.Join(downloadsDir, "test_success.txt")
	tmpPath := finalPath + ".tmp"

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("temporary file %s should have been removed/renamed", tmpPath)
	}

	content, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("failed to read output file %s: %v", finalPath, err)
	}
	if !bytes.Equal(content, payload) {
		t.Errorf("payload mismatch: got %s, want %s", string(content), string(payload))
	}
}

func TestReceive_InterruptedTransfer(t *testing.T) {
	downloadsDir := "downloads"
	defer os.RemoveAll(downloadsDir)

	hdr := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_interrupted.txt",
		FileSize: 1000,
		Checksum: [32]byte{},
	}

	var hdrBuf bytes.Buffer
	if err := hdr.WriteTo(&hdrBuf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	fr := &faultReader{
		headerData: hdrBuf.Bytes(),
		failAfter:  200, // fail after receiving 200 bytes out of 1000
	}

	ms := &mockStream{reader: fr}
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected Receive to fail due to simulated network failure")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on failure")
	}

	finalPath := filepath.Join(downloadsDir, "test_interrupted.txt")
	tmpPath := finalPath + ".tmp"

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("temporary file %s should have been deleted on error", tmpPath)
	}
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("final file %s should not exist on error", finalPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	downloadsDir := "downloads"
	defer os.RemoveAll(downloadsDir)

	payload := []byte("data with corrupt checksum")
	hdr := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_corrupt.txt",
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{1, 2, 3}, // invalid checksum
	}

	var buf bytes.Buffer
	if err := hdr.WriteTo(&buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(payload)

	ms := &mockStream{reader: &buf}
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on checksum mismatch")
	}

	finalPath := filepath.Join(downloadsDir, "test_corrupt.txt")
	tmpPath := finalPath + ".tmp"

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("temporary file %s should have been deleted on checksum mismatch", tmpPath)
	}
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("final file %s should not exist on checksum mismatch", finalPath)
	}
}

func TestReceive_HeaderError(t *testing.T) {
	downloadsDir := "downloads"
	defer os.RemoveAll(downloadsDir)

	ms := &mockStream{reader: bytes.NewReader([]byte{0x01})} // truncated header
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on header read failure")
	}

	if !ms.reset {
		t.Errorf("expected stream.Reset() to be called on header error")
	}
}
