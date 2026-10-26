package chunk_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
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
func (m *mockPipeStream) Protocol() libp2p_protocol.ID { return protocol.ChunkTransportProtocolID }
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

func createStreamPipe(peer1, peer2 peer.ID) (network.Stream, network.Stream) {
	c1, c2 := net.Pipe()
	return &mockPipeStream{netConn: c1, remotePeer: peer2}, &mockPipeStream{netConn: c2, remotePeer: peer1}
}

func TestStreamHandler_ReadDeadline_SilentPeer(t *testing.T) {
	oldReadDeadline := chunk.DefaultReadDeadline
	chunk.DefaultReadDeadline = 50 * time.Millisecond
	defer func() { chunk.DefaultReadDeadline = oldReadDeadline }()

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	s1, s2 := createStreamPipe(p1, p2)
	defer s2.Close()

	eng1 := createTestEngine(t)
	handler := chunk.NewStreamHandler(nil, eng1)

	done := make(chan struct{})
	go func() {
		handler.HandleStream(s1)
		close(done)
	}()

	select {
	case <-done:
		// Stream handler terminated after read deadline expired
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Stream handler did not terminate within read deadline window")
	}

	// Verify s2 receives EOF because s1 was closed
	buf := make([]byte, 10)
	_, err := s2.Read(buf)
	if err == nil {
		t.Fatalf("Expected error reading from closed peer stream, got nil")
	}
}

func TestStreamHandler_ACKDeadline_SilentACKPeer(t *testing.T) {
	oldACKDeadline := chunk.DefaultACKDeadline
	chunk.DefaultACKDeadline = 50 * time.Millisecond
	defer func() { chunk.DefaultACKDeadline = oldACKDeadline }()

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	s1, s2 := createStreamPipe(p1, p2)
	defer s2.Close()

	eng1 := createTestEngine(t)
	handler := chunk.NewStreamHandler(nil, eng1)

	ctx := context.Background()
	data := []byte("hello world chunk data for deadline test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		handler.HandleStream(s1)
		close(done)
	}()

	// Send REQUEST_CHUNK from client (s2)
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s2, req); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Read CHUNK response on client
	resp, err := chunk.ReadMessage(s2)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK message, got %d", resp.Type)
	}

	// Client intentionally DOES NOT send ACK.
	// Wait for handler to hit ACK timeout and terminate.
	select {
	case <-done:
		// Stream handler terminated after ACK deadline expired
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Stream handler did not terminate within ACK deadline window")
	}

	buf := make([]byte, 10)
	_, err = s2.Read(buf)
	if err == nil {
		t.Fatalf("Expected stream to be closed after ACK timeout, got nil error")
	}
}

type dummyStream struct {
	io.ReadWriter
	writeDeadlineSet bool
	readDeadlineSet  bool
}

func (d *dummyStream) SetWriteDeadline(t time.Time) error {
	d.writeDeadlineSet = true
	return nil
}

func (d *dummyStream) SetReadDeadline(t time.Time) error {
	d.readDeadlineSet = true
	return nil
}

func TestWriteMessage_EnforcesWriteDeadline(t *testing.T) {
	var buf bytes.Buffer
	ds := &dummyStream{ReadWriter: &buf}

	msg := chunk.BuildAck([32]byte{}, 0)
	if err := chunk.WriteMessage(ds, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	if !ds.writeDeadlineSet {
		t.Errorf("Expected SetWriteDeadline to be called on stream before writing message")
	}
}
