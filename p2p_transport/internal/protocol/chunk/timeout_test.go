package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})
	addrInfo := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(context.Background(), addrInfo); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestChunkProtocol_ServerStalledRequestTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)
	handler.ControlTimeout = 100 * time.Millisecond

	// Open stream from h2 to h1 and do nothing
	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Wait for handler's read deadline to trigger and reset stream
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 100)
		_, _ = s.Read(buf)
		close(done)
	}()

	select {
	case <-done:
		// Stream was reset by server due to read timeout
	case <-time.After(1 * time.Second):
		t.Fatal("Expected server to close stream on request timeout, but it remained open")
	}
}

func TestChunkProtocol_ServerStalledAckTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	handler := chunk.NewStreamHandler(h1, eng1)
	handler.ControlTimeout = 100 * time.Millisecond

	ctx := context.Background()
	data := make([]byte, 512)
	rand.Read(data)
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Peer 2 sends REQUEST_CHUNK, reads the CHUNK response, but stalls before sending ACK
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	reqMsg := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, reqMsg); err != nil {
		t.Fatalf("Failed to write request chunk: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp.Type)
	}

	// Now stall (do not send ACK)
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 100)
		_, _ = s.Read(buf)
		close(done)
	}()

	select {
	case <-done:
		// Server timed out reading ACK and reset stream
	case <-time.After(1 * time.Second):
		t.Fatal("Expected server to close stream on ACK timeout, but it remained open")
	}
}

func TestChunkProtocol_ClientTimeoutOnStalledServer(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng2 := createTestEngine(t)

	// Set up a stalling handler on h1
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		// Server accepts stream, reads request, then stalls without writing response
		defer s.Close()
		_, _ = chunk.ReadMessage(s)
		time.Sleep(1 * time.Second)
	})

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	client.ControlTimeout = 100 * time.Millisecond
	client.ChunkTimeout = 100 * time.Millisecond

	var dummyID core.ContentID
	_, err = client.Resolve(context.Background(), dummyID)
	if err == nil {
		t.Fatal("Expected client Resolve to time out, got nil error")
	}
	if !strings.Contains(err.Error(), "i/o timeout") && !strings.Contains(err.Error(), "failed to read response") {
		t.Errorf("Unexpected error message: %v", err)
	}

	var dummyChunkID core.ChunkID
	_, err = client.FetchChunk(context.Background(), dummyChunkID)
	if err == nil {
		t.Fatal("Expected client FetchChunk to time out, got nil error")
	}
	if !strings.Contains(err.Error(), "i/o timeout") && !strings.Contains(err.Error(), "failed to read response") {
		t.Errorf("Unexpected error message: %v", err)
	}
}
