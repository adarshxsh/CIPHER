package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"

	"cipher/network/content/manifest"
	"cipher/network/protocol"
	"cipher/network/protocol/chunk"
)

type mockPipeStream struct {
	netConn    net.Conn
	remotePeer peer.ID
}

func (m *mockPipeStream) Read(p []byte) (n int, err error)  { return m.netConn.Read(p) }
func (m *mockPipeStream) Write(p []byte) (n int, err error) { return m.netConn.Write(p) }
func (m *mockPipeStream) Close() error                      { return m.netConn.Close() }
func (m *mockPipeStream) SetDeadline(t time.Time) error     { return m.netConn.SetDeadline(t) }
func (m *mockPipeStream) SetReadDeadline(t time.Time) error  { return m.netConn.SetReadDeadline(t) }
func (m *mockPipeStream) SetWriteDeadline(t time.Time) error { return m.netConn.SetWriteDeadline(t) }

func (m *mockPipeStream) ID() string                                  { return "test-stream-id" }
func (m *mockPipeStream) Reset() error                                { return m.netConn.Close() }
func (m *mockPipeStream) ResetWithError(network.StreamErrorCode) error { return m.netConn.Close() }
func (m *mockPipeStream) CloseWrite() error                           { return m.netConn.Close() }
func (m *mockPipeStream) CloseRead() error                            { return m.netConn.Close() }
func (m *mockPipeStream) Stat() network.Stats                        { return network.Stats{} }
func (m *mockPipeStream) Protocol() libp2p_protocol.ID                { return protocol.ChunkTransportProtocolID }
func (m *mockPipeStream) SetProtocol(libp2p_protocol.ID) error       { return nil }
func (m *mockPipeStream) Conn() network.Conn                          { return &mockPipeConn{remotePeer: m.remotePeer} }
func (m *mockPipeStream) Scope() network.StreamScope                  { return &network.NullScope{} }

type mockPipeConn struct {
	network.Conn
	remotePeer peer.ID
}

func (c *mockPipeConn) RemotePeer() peer.ID { return c.remotePeer }
func (c *mockPipeConn) LocalPeer() peer.ID  { return "local-peer" }

func TestChunkProtocol_ReadDeadlineTimeout(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, _ := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	handler := chunk.NewStreamHandler(h1, eng1)

	clientConn, serverConn := net.Pipe()
	sServer := &mockPipeStream{netConn: serverConn, remotePeer: "test-client"}
	sClient := &mockPipeStream{netConn: clientConn, remotePeer: h1.ID()}

	// Start server handleStream on server side of pipe
	go handler.HandleStreamForTest(sServer)

	// Do NOT write anything on client side; let stream remain idle.
	// Server read deadline should trigger after 100ms and close stream.
	buf := make([]byte, 100)
	readDone := make(chan error, 1)
	go func() {
		_, err := sClient.Read(buf)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatalf("Expected error reading from timed-out stream, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for idle stream to be closed by server read deadline")
	}
}

func TestChunkProtocol_AckReadDeadlineTimeout(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 100 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, _ := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	handler := chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]

	clientConn, serverConn := net.Pipe()
	sServer := &mockPipeStream{netConn: serverConn, remotePeer: "test-client"}
	sClient := &mockPipeStream{netConn: clientConn, remotePeer: h1.ID()}

	go handler.HandleStreamForTest(sServer)

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(sClient, req); err != nil {
		t.Fatalf("Failed to send REQUEST_CHUNK: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(sClient)
	if err != nil {
		t.Fatalf("Failed to read CHUNK response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp.Type)
	}

	// Do NOT send ACK. Server is waiting for ACK with a read deadline of 100ms.
	buf := make([]byte, 100)
	readDone := make(chan error, 1)
	go func() {
		_, err := sClient.Read(buf)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatalf("Expected error reading from stream after ACK deadline expired, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for server to close stream on missing ACK deadline")
	}
}
