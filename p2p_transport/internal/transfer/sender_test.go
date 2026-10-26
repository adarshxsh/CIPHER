package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"net"
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
	return peer.ID("mock-peer-id")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/12345")
	return ma
}

type mockStream struct {
	netConn net.Conn
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.netConn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.netConn.Write(p)
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func (m *mockStream) Close() error {
	return m.netConn.Close()
}

func (m *mockStream) Reset() error {
	return m.netConn.Close()
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return m.netConn.Close()
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return m.netConn.SetDeadline(t)
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return m.netConn.SetReadDeadline(t)
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return m.netConn.SetWriteDeadline(t)
}

func (m *mockStream) ID() string {
	return "mock-stream-id"
}

func (m *mockStream) Protocol() libp2p_protocol.ID {
	return libp2p_protocol.ID("/cipher/filetransfer/1.0.0")
}

func (m *mockStream) SetProtocol(p libp2p_protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Scope() network.StreamScope {
	return nil
}

func TestSendAndReceive_InlineChecksum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "transfer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testData := make([]byte, 100*1024)
	if _, err := rand.Read(testData); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}
	expectedChecksum := sha256.Sum256(testData)

	testFilePath := filepath.Join(tmpDir, "test_file.bin")
	if err := os.WriteFile(testFilePath, testData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	senderStream := &mockStream{netConn: clientConn}
	receiverStream := &mockStream{netConn: serverConn}

	errCh := make(chan error, 1)
	go func() {
		errCh <- Receive(receiverStream)
	}()

	if err := Send(senderStream, testFilePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	receivedFilePath := filepath.Join("downloads", filepath.Base(testFilePath))
	defer os.Remove(receivedFilePath)

	receivedData, err := os.ReadFile(receivedFilePath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(receivedData, testData) {
		t.Fatalf("received content mismatch")
	}

	receivedChecksum := sha256.Sum256(receivedData)
	if receivedChecksum != expectedChecksum {
		t.Fatalf("checksum mismatch: expected %x, got %x", expectedChecksum, receivedChecksum)
	}
}

func TestSend_NonExistentFile(t *testing.T) {
	clientConn, _ := net.Pipe()
	senderStream := &mockStream{netConn: clientConn}
	defer senderStream.Close()

	err := Send(senderStream, "non_existent_file_path.xyz")
	if err == nil {
		t.Fatalf("expected error sending non-existent file, got nil")
	}
}
