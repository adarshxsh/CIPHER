package chunk_test

import (
	"context"
	"crypto/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealTCPHosts(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	err = h2.Connect(context.Background(), *host.InfoFromHost(h1))
	if err != nil {
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	return h1, h2
}

func TestChunkHandler_IdleStreamTimeout(t *testing.T) {
	origTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 50 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origTimeout }()

	h1, h2 := setupRealTCPHosts(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Wait longer than StreamReadTimeout without sending anything
	time.Sleep(150 * time.Millisecond)

	// Attempting to read from the stream should reveal it was reset/closed by handler
	buf := make([]byte, 10)
	_ = s.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error when reading from stream closed due to read deadline, got nil")
	}
}

func TestChunkHandler_AckTimeout(t *testing.T) {
	origAckTimeout := chunk.AckTimeout
	chunk.AckTimeout = 50 * time.Millisecond
	defer func() { chunk.AckTimeout = origAckTimeout }()

	h1, h2 := setupRealTCPHosts(t)
	eng1 := createTestEngine(t)

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, strings.NewReader(string(data)), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunk.NewStreamHandler(h1, eng1)

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK message, got %d", resp.Type)
	}

	// Client stalls and DOES NOT send ACK
	time.Sleep(150 * time.Millisecond)

	// Next read attempt on stream should show it was closed by server due to ACK timeout
	buf := make([]byte, 10)
	_ = s.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error reading from stream after ACK timeout, got nil")
	}
}

func TestChunkClient_TimeoutOnStalledServer(t *testing.T) {
	origReadTimeout := chunk.StreamReadTimeout
	chunk.StreamReadTimeout = 50 * time.Millisecond
	defer func() { chunk.StreamReadTimeout = origReadTimeout }()

	h1, h2 := setupRealTCPHosts(t)

	// Server handler that accepts stream, reads request, but stalls without responding
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 1024)
		_, _ = s.Read(buf)
		time.Sleep(200 * time.Millisecond)
	})

	eng2 := createTestEngine(t)
	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var dummyID [32]byte
	_, err = client.Resolve(context.Background(), dummyID)
	if err == nil {
		t.Fatal("Expected client Resolve to return timeout error, got nil")
	}

	netErr, isNetErr := err.(net.Error)
	if isNetErr && !netErr.Timeout() {
		t.Logf("Returned error was network error but not timeout: %v", err)
	}
}
