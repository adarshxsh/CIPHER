package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

// mockStream wraps net.Conn to fulfill libp2p network.Stream interface for testing.
type mockStream struct {
	conn net.Conn
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.conn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.conn.Write(p)
}

func (m *mockStream) Close() error {
	return m.conn.Close()
}

func (m *mockStream) Reset() error {
	return m.conn.Close()
}

func (m *mockStream) ResetWithError(network.StreamErrorCode) error {
	return m.conn.Close()
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func (m *mockStream) ID() string {
	return "mock-stream-1"
}

func (m *mockStream) Protocol() protocol.ID {
	return "/cipher/filetransfer/1.0.0"
}

func (m *mockStream) SetProtocol(p protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Scope() network.StreamScope {
	return &network.NullScope{}
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return m.conn.SetDeadline(t)
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return m.conn.SetReadDeadline(t)
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return m.conn.SetWriteDeadline(t)
}

type mockConn struct {
	network.Conn
}

func (mc *mockConn) RemotePeer() peer.ID {
	return peer.ID("mock-peer-id")
}

func (mc *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	m, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return m
}

func TestSendAndReceive_SinglePass(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "testdata.txt")
	testData := []byte("Hello, Single-Pass Checksum Streaming Trailer Test!")
	if err := os.WriteFile(srcFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get wd: %v", err)
	}
	defer os.Chdir(origDir)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to chdir: %v", err)
	}

	c1, c2 := net.Pipe()
	s1 := &mockStream{conn: c1}
	s2 := &mockStream{conn: c2}

	errChan := make(chan error, 2)

	go func() {
		errChan <- Send(s1, srcFile)
	}()

	go func() {
		errChan <- Receive(s2)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Unexpected error during transfer: %v", err)
		}
	}

	receivedFile := filepath.Join(tmpDir, "downloads", filepath.Base(srcFile))
	content, err := os.ReadFile(receivedFile)
	if err != nil {
		t.Fatalf("Failed to read received file: %v", err)
	}
	if !bytes.Equal(content, testData) {
		t.Errorf("Received content mismatch: expected %q, got %q", string(testData), string(content))
	}
}

func TestSend_ZeroHeaderChecksumAndPostPayloadTrailer(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "payload.bin")
	payload := []byte("Chunk payload data for single-pass verification test.")
	if err := os.WriteFile(srcFile, payload, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	c1, c2 := net.Pipe()
	s1 := &mockStream{conn: c1}
	s2 := &mockStream{conn: c2}

	go func() {
		_ = Send(s1, srcFile)
	}()

	var header Header
	if err := header.ReadFrom(s2); err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	if !header.IsZeroChecksum() {
		t.Fatalf("Expected header checksum to be zeroed for single pass, got %x", header.Checksum)
	}

	buf := make([]byte, header.FileSize)
	if _, err := io.ReadFull(s2, buf); err != nil {
		t.Fatalf("Failed to read payload: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("Payload mismatch")
	}

	hasher := sha256.New()
	hasher.Write(buf)
	expectedHash := hasher.Sum(nil)

	var trailer [32]byte
	if _, err := io.ReadFull(s2, trailer[:]); err != nil {
		t.Fatalf("Failed to read 32-byte trailer: %v", err)
	}

	if !bytes.Equal(trailer[:], expectedHash) {
		t.Errorf("Checksum trailer mismatch: expected %x, got %x", expectedHash, trailer)
	}

	s2.Close()
}

func TestReceive_LegacyMode(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	_ = os.Chdir(tmpDir)

	payload := []byte("Legacy header checksum data payload")
	hasher := sha256.New()
	hasher.Write(payload)
	var legacyChecksum [32]byte
	copy(legacyChecksum[:], hasher.Sum(nil))

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "legacy.txt",
		FileSize: uint64(len(payload)),
		Checksum: legacyChecksum,
	}

	c1, c2 := net.Pipe()
	s1 := &mockStream{conn: c1}
	s2 := &mockStream{conn: c2}

	errChan := make(chan error, 1)

	go func() {
		if err := header.WriteTo(s1); err != nil {
			errChan <- err
			return
		}
		if _, err := s1.Write(payload); err != nil {
			errChan <- err
			return
		}
		s1.Close()
		errChan <- nil
	}()

	recvErr := Receive(s2)
	senderErr := <-errChan

	if senderErr != nil {
		t.Fatalf("Sender error in legacy test: %v", senderErr)
	}
	if recvErr != nil {
		t.Fatalf("Receive failed in legacy mode: %v", recvErr)
	}

	receivedContent, err := os.ReadFile(filepath.Join(tmpDir, "downloads", "legacy.txt"))
	if err != nil {
		t.Fatalf("Failed to read received file: %v", err)
	}
	if !bytes.Equal(receivedContent, payload) {
		t.Errorf("Received content mismatch in legacy test")
	}
}

func TestReceive_TrailerMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	_ = os.Chdir(tmpDir)

	payload := []byte("Valid payload content")
	badTrailer := [32]byte{1, 2, 3, 4, 5}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "bad_trailer.txt",
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}

	c1, c2 := net.Pipe()
	s1 := &mockStream{conn: c1}
	s2 := &mockStream{conn: c2}

	go func() {
		_ = header.WriteTo(s1)
		_, _ = s1.Write(payload)
		_, _ = s1.Write(badTrailer[:])
		_ = s1.Close()
	}()

	err := Receive(s2)
	if err == nil {
		t.Errorf("Expected Receive to fail on corrupted trailer, but got nil")
	}
}
