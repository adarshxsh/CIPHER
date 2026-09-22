package chunk_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	coreprotocol "github.com/libp2p/go-libp2p/core/protocol"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer")
}

type mockStream struct {
	netConn net.Conn
}

func newMockStream(c net.Conn) *mockStream {
	return &mockStream{netConn: c}
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.netConn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.netConn.Write(p)
}

func (m *mockStream) Close() error {
	return m.netConn.Close()
}

func (m *mockStream) CloseRead() error {
	return m.netConn.Close()
}

func (m *mockStream) CloseWrite() error {
	return m.netConn.Close()
}

func (m *mockStream) Reset() error {
	return m.netConn.Close()
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return m.netConn.Close()
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

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) ID() string {
	return "1"
}

func (m *mockStream) Protocol() coreprotocol.ID {
	return coreprotocol.ID(protocol.ChunkTransportProtocolID)
}

func (m *mockStream) SetProtocol(id coreprotocol.ID) error {
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func (m *mockStream) Scope() network.StreamScope {
	return &network.NullScope{}
}

func TestHandleStream_ExceedMaxTransactionsPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	var dummyID core.ContentID

	// Send two requests on the same stream to exceed MaxTransactionsPerStream (1)
	req1 := chunk.BuildRequestManifest(dummyID)
	req2 := chunk.BuildRequestManifest(dummyID)

	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to send transaction 1: %v", err)
	}
	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response for transaction 1: %v", err)
	}
	if resp1.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for unknown content, got type %d", resp1.Type)
	}

	if err := chunk.WriteMessage(s, req2); err != nil {
		t.Fatalf("Failed to send transaction 2: %v", err)
	}
	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read response for transaction 2: %v", err)
	}

	if resp2.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for transaction 2 exceeding limit, got type %d", resp2.Type)
	}

	code, msg, err := chunk.ParseError(resp2.Payload)
	if err != nil {
		t.Fatalf("Failed to parse error payload: %v", err)
	}

	if code != chunk.ErrBadRequest {
		t.Errorf("Expected ErrorCode ErrBadRequest (%d), got %d", chunk.ErrBadRequest, code)
	}
	if msg != "exceeded maximum transactions per stream" {
		t.Errorf("Expected error message 'exceeded maximum transactions per stream', got '%s'", msg)
	}
}

func TestHandleStream_ExceedMaxMessagesPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send messages that do not complete a transaction to test message limit.
	// MaxMessagesPerStream = 4. Sending 5 messages should cause handler to terminate stream.
	for i := 0; i < 5; i++ {
		msg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    0x99, // Unsupported message type
			Payload: []byte{},
		}
		if err := chunk.WriteMessage(s, msg); err != nil {
			// Stream was closed by handler on exceeding message limit
			return
		}
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			// Stream terminated as expected
			return
		}
		if resp.Type != chunk.MsgError {
			t.Fatalf("Expected MsgError, got type %d", resp.Type)
		}
	}

	// Next read should return EOF or stream closed error because max messages was exceeded
	_, err = chunk.ReadMessage(s)
	if err == nil {
		t.Fatalf("Expected stream termination after exceeding MaxMessagesPerStream")
	}
}

func TestHandleStream_ReadDeadlineTimeout(t *testing.T) {
	origDeadline := chunk.StreamReadDeadline
	chunk.StreamReadDeadline = 50 * time.Millisecond
	defer func() { chunk.StreamReadDeadline = origDeadline }()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	serverStream := newMockStream(serverConn)
	eng := createTestEngine(t)
	handler := chunk.NewStreamHandler(setupMockNetworkHost(t), eng)

	done := make(chan struct{})
	go func() {
		// handleStream should set read deadline, wait 50ms, time out, and close stream
		handler.HandleStreamForTest(serverStream)
		close(done)
	}()

	// Read on client side; should receive EOF when handleStream times out and closes stream
	buf := make([]byte, 1)
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Fatalf("Expected client read to fail after host deadline expiration")
	}

	select {
	case <-done:
		// Stream handler terminated and cleaned up as expected
	case <-time.After(2 * time.Second):
		t.Fatalf("handleStream did not exit after read deadline timeout")
	}
}

func setupMockNetworkHost(t testing.TB) host.Host {
	h1, _ := setupMockNetwork(t)
	return h1
}
