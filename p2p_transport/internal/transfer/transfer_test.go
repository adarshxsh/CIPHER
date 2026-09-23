package transfer_test

import (
	"bytes"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/transfer"
)

type mockConn struct {
	network.Conn
}

func (c *mockConn) RemotePeer() peer.ID {
	return peer.ID("mockPeer")
}

func (c *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	rawConn net.Conn
	mConn   *mockConn
}

func newMockStreamPair() (*mockStream, *mockStream) {
	c1, c2 := net.Pipe()
	conn := &mockConn{}
	s1 := &mockStream{rawConn: c1, mConn: conn}
	s2 := &mockStream{rawConn: c2, mConn: conn}
	return s1, s2
}

func (s *mockStream) Read(p []byte) (int, error) {
	return s.rawConn.Read(p)
}

func (s *mockStream) Write(p []byte) (int, error) {
	return s.rawConn.Write(p)
}

func (s *mockStream) Close() error {
	return s.rawConn.Close()
}

func (s *mockStream) LocalAddr() net.Addr {
	return s.rawConn.LocalAddr()
}

func (s *mockStream) RemoteAddr() net.Addr {
	return s.rawConn.RemoteAddr()
}

func (s *mockStream) SetDeadline(t time.Time) error {
	return s.rawConn.SetDeadline(t)
}

func (s *mockStream) SetReadDeadline(t time.Time) error {
	return s.rawConn.SetReadDeadline(t)
}

func (s *mockStream) SetWriteDeadline(t time.Time) error {
	return s.rawConn.SetWriteDeadline(t)
}

func (s *mockStream) Conn() network.Conn {
	return s.mConn
}

func (s *mockStream) Reset() error {
	return s.Close()
}

func (s *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return s.Close()
}

func (s *mockStream) CloseWrite() error {
	return nil
}

func (s *mockStream) CloseRead() error {
	return nil
}

func (s *mockStream) Protocol() protocol.ID {
	return "/cipher/filetransfer/1.0.0"
}

func (s *mockStream) SetProtocol(p protocol.ID) error {
	return nil
}

func (s *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (s *mockStream) ID() string {
	return "mock-stream-1"
}

func (s *mockStream) Scope() network.StreamScope {
	return &network.NullScope{}
}

func TestSendAndReceive_SinglePass(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "transfer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	defer os.Chdir(origWD)

	testFileName := "test_payload.bin"
	payload := make([]byte, 1024*1024) // 1 MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	if err := os.WriteFile(testFileName, payload, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	s1, s2 := newMockStreamPair()

	sendErrChan := make(chan error, 1)
	go func() {
		sendErrChan <- transfer.Send(s1, testFileName)
	}()

	recvErr := transfer.Receive(s2)
	if recvErr != nil {
		t.Fatalf("Receive failed: %v", recvErr)
	}

	if sendErr := <-sendErrChan; sendErr != nil {
		t.Fatalf("Send failed: %v", sendErr)
	}

	receivedPath := filepath.Join("downloads", testFileName)
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(payload, receivedData) {
		t.Fatalf("received file content does not match original payload")
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "transfer_test_mismatch_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	defer os.Chdir(origWD)

	s1, s2 := newMockStreamPair()

	go func() {
		defer s1.Close()
		filename := "corrupt.txt"
		fileData := []byte("hello world")
		header := &transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: filename,
			FileSize: uint64(len(fileData)),
			Checksum: [32]byte{}, // Trailing checksum mode
		}
		if err := header.WriteTo(s1); err != nil {
			return
		}
		s1.Write(fileData)
		// Transmit wrong trailing checksum
		var badChecksum [32]byte
		badChecksum[0] = 0xFF
		s1.Write(badChecksum[:])
	}()

	recvErr := transfer.Receive(s2)
	if recvErr == nil {
		t.Fatalf("expected Receive to fail on checksum mismatch, got nil")
	}
}

func TestReceive_BackwardCompatibility_PrecomputedHeaderChecksum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "transfer_test_compat_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current wd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to temp dir: %v", err)
	}
	defer os.Chdir(origWD)

	s1, s2 := newMockStreamPair()

	filename := "legacy.txt"
	fileData := []byte("legacy precalculated header checksum payload")
	hasher := sha256.New()
	hasher.Write(fileData)
	var expectedChecksum [32]byte
	copy(expectedChecksum[:], hasher.Sum(nil))

	go func() {
		defer s1.Close()
		header := &transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: filename,
			FileSize: uint64(len(fileData)),
			Checksum: expectedChecksum, // Precomputed header checksum
		}
		if err := header.WriteTo(s1); err != nil {
			return
		}
		s1.Write(fileData)
		// Note: No trailing checksum sent for legacy mode
	}()

	recvErr := transfer.Receive(s2)
	if recvErr != nil {
		t.Fatalf("Receive failed in backward compatibility mode: %v", recvErr)
	}

	receivedPath := filepath.Join("downloads", filename)
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(fileData, receivedData) {
		t.Fatalf("received file content does not match original legacy payload")
	}
}
