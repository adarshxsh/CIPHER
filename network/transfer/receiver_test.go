package transfer

import (
	"bytes"
	"crypto/sha256"
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
	return peer.ID("test-peer-id")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return maddr
}

type mockStream struct {
	r           *bytes.Buffer
	w           *bytes.Buffer
	closeCalled bool
	resetCalled bool
}

func newMockStream(r *bytes.Buffer) *mockStream {
	if r == nil {
		r = new(bytes.Buffer)
	}
	return &mockStream{
		r: r,
		w: new(bytes.Buffer),
	}
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	return m.r.Read(p)
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	return m.w.Write(p)
}

func (m *mockStream) Close() error {
	m.closeCalled = true
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return m.Reset()
}

func (m *mockStream) SetDeadline(t time.Time) error      { return nil }
func (m *mockStream) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockStream) SetWriteDeadline(t time.Time) error { return nil }

func (m *mockStream) ID() string                              { return "mock-stream-id" }
func (m *mockStream) Protocol() libp2p_protocol.ID            { return libp2p_protocol.ID("/cipher/transfer") }
func (m *mockStream) SetProtocol(id libp2p_protocol.ID) error { return nil }
func (m *mockStream) Stat() network.Stats                     { return network.Stats{} }
func (m *mockStream) Conn() network.Conn                      { return &mockConn{} }
func (m *mockStream) Scope() network.StreamScope              { return nil }

func TestReceiveSuccess(t *testing.T) {
	filename := "test_success.txt"
	data := []byte("Hello, CIPHER P2P transfer!")
	checksum := sha256.Sum256(data)

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(data)),
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(data)

	s := newMockStream(buf)
	err := Receive(s)

	if err != nil {
		t.Fatalf("expected Receive to succeed, got error: %v", err)
	}
	if !s.closeCalled {
		t.Errorf("expected s.Close() to be called on success")
	}
	if s.resetCalled {
		t.Errorf("expected s.Reset() NOT to be called on success")
	}

	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	downloaded, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("expected output file to exist, failed to read: %v", err)
	}
	if !bytes.Equal(downloaded, data) {
		t.Errorf("downloaded content mismatch: expected %q, got %q", data, downloaded)
	}
}

func TestReceiveHeaderError(t *testing.T) {
	// Truncated header
	buf := bytes.NewBuffer([]byte{0x01})
	s := newMockStream(buf)

	err := Receive(s)
	if err == nil {
		t.Fatalf("expected Receive to fail on truncated header")
	}
	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on header error")
	}
}

func TestReceiveInterruptedData(t *testing.T) {
	filename := "test_interrupted.txt"
	data := []byte("Full expected content")
	checksum := sha256.Sum256(data)

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(data)),
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	// Write partial data
	buf.Write(data[:5])

	s := newMockStream(buf)
	err := Receive(s)

	if err == nil {
		t.Fatalf("expected Receive to fail on interrupted data stream")
	}
	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on interrupted stream")
	}

	outPath := filepath.Join("downloads", filename)
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		os.Remove(outPath)
		t.Errorf("expected partial file %s to be deleted after error", outPath)
	}
}

func TestReceiveChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	data := []byte("Actual payload content")
	invalidChecksum := [32]byte{1, 2, 3, 4}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(data)),
		Checksum: invalidChecksum,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(data)

	s := newMockStream(buf)
	err := Receive(s)

	if err == nil {
		t.Fatalf("expected Receive to fail on checksum mismatch")
	}
	if !s.resetCalled {
		t.Errorf("expected s.Reset() to be called on checksum mismatch")
	}

	outPath := filepath.Join("downloads", filename)
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		os.Remove(outPath)
		t.Errorf("expected corrupted file %s to be deleted after checksum mismatch", outPath)
	}
}
