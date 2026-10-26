package transfer_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/transfer"
)

type mockPipeStream struct {
	netConn    net.Conn
	remotePeer peer.ID
}

func (m *mockPipeStream) Read(p []byte) (n int, err error) { return m.netConn.Read(p) }
func (m *mockPipeStream) Write(p []byte) (n int, err error) { return m.netConn.Write(p) }
func (m *mockPipeStream) Close() error { return m.netConn.Close() }
func (m *mockPipeStream) CloseRead() error { return nil }
func (m *mockPipeStream) CloseWrite() error { return nil }
func (m *mockPipeStream) Reset() error { return m.netConn.Close() }
func (m *mockPipeStream) ResetWithError(code network.StreamErrorCode) error { return m.netConn.Close() }
func (m *mockPipeStream) SetDeadline(t time.Time) error { return m.netConn.SetDeadline(t) }
func (m *mockPipeStream) SetReadDeadline(t time.Time) error { return m.netConn.SetReadDeadline(t) }
func (m *mockPipeStream) SetWriteDeadline(t time.Time) error { return m.netConn.SetWriteDeadline(t) }
func (m *mockPipeStream) Protocol() libp2p_protocol.ID { return "/cipher/filetransfer/1.0.0" }
func (m *mockPipeStream) SetProtocol(libp2p_protocol.ID) error { return nil }
func (m *mockPipeStream) Stat() network.Stats { return network.Stats{} }
func (m *mockPipeStream) ID() string { return "test-pipe" }
func (m *mockPipeStream) Conn() network.Conn { return &mockPipeConn{remotePeer: m.remotePeer} }
func (m *mockPipeStream) Scope() network.StreamScope { return nil }

type mockPipeConn struct {
	network.Conn
	remotePeer peer.ID
}

func (c *mockPipeConn) RemotePeer() peer.ID { return c.remotePeer }
func (c *mockPipeConn) RemoteMultiaddr() multiaddr.Multiaddr { return nil }

func createStreamPipe(peer1, peer2 peer.ID) (network.Stream, network.Stream) {
	c1, c2 := net.Pipe()
	return &mockPipeStream{netConn: c1, remotePeer: peer2}, &mockPipeStream{netConn: c2, remotePeer: peer1}
}

func TestReceive_HeaderReadDeadline(t *testing.T) {
	oldReadDeadline := transfer.DefaultReadDeadline
	transfer.DefaultReadDeadline = 50 * time.Millisecond
	defer func() { transfer.DefaultReadDeadline = oldReadDeadline }()

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	s1, s2 := createStreamPipe(p1, p2)
	defer s2.Close()

	err := transfer.Receive(s1)
	if err == nil {
		t.Fatalf("Expected error from Receive on timeout, got nil")
	}
}

func TestSendAndReceive_SuccessfulTransfer(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "testdata.txt")
	content := []byte("hello file transfer with deadlines")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	s1, s2 := createStreamPipe(p1, p2)

	errCh := make(chan error, 2)

	go func() {
		errCh <- transfer.Receive(s2)
	}()

	go func() {
		errCh <- transfer.Send(s1, testFile)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Transfer failed: %v", err)
		}
	}

	// Verify received file in downloads dir
	downloadedFile := filepath.Join("downloads", "testdata.txt")
	defer os.RemoveAll("downloads")

	data, err := os.ReadFile(downloadedFile)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(content), string(data))
	}
}
