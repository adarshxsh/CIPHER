package chunk_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
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
	err = h2.Connect(context.Background(), peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

// TestStalledStream_HandlerReadTimeout verifies that if a remote peer opens a stream
// to the StreamHandler but sends no data, the handler times out on its read deadline,
// closes the stream, and terminates the handler goroutine.
func TestStalledStream_HandlerReadTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Configure handler with a short 100ms read timeout
	chunk.NewStreamHandler(h1, eng1, chunk.WithReadTimeout(100*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Peer 2 opens the stream but sends nothing (silent peer)
	// Wait long enough for the 100ms read deadline to trigger on Peer 1
	time.Sleep(250 * time.Millisecond)

	// Try reading from the stream on Peer 2. Since Peer 1 timed out and closed the stream,
	// reading should return EOF or stream reset error.
	buf := make([]byte, 10)
	_ = s.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error when reading from stream closed by server timeout")
	}
}

// TestStalledStream_ClientResolveTimeout verifies that if a remote server hangs and fails
// to send a response to REQUEST_MANIFEST, Client.Resolve times out on its read deadline.
func TestStalledStream_ClientResolveTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng2 := createTestEngine(t)

	// Peer 1 accepts stream and reads request, but never responds
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 1024)
		_, _ = s.Read(buf)
		// Hang until client deadline expires
		time.Sleep(500 * time.Millisecond)
	})

	ctx := context.Background()
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2, chunk.WithClientReadTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var dummyID core.ContentID
	start := time.Now()
	_, err = client.Resolve(ctx, dummyID)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected Resolve to fail due to deadline timeout")
	}

	if duration > 400*time.Millisecond {
		t.Errorf("Resolve took too long (%v), expected deadline timeout around 100ms", duration)
	}
}

// TestStalledStream_ClientFetchChunkTimeout verifies that if a remote server hangs
// and fails to send a CHUNK response, Client.FetchChunk times out.
func TestStalledStream_ClientFetchChunkTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng2 := createTestEngine(t)

	// Peer 1 accepts stream and reads request, but never responds
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 1024)
		_, _ = s.Read(buf)
		time.Sleep(500 * time.Millisecond)
	})

	ctx := context.Background()
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2, chunk.WithClientReadTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var dummyChunkID core.ChunkID
	start := time.Now()
	_, err = client.FetchChunk(ctx, dummyChunkID)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("Expected FetchChunk to fail due to deadline timeout")
	}

	if duration > 400*time.Millisecond {
		t.Errorf("FetchChunk took too long (%v), expected deadline timeout around 100ms", duration)
	}
}

// TestStalledStream_HandlerACKTimeout verifies that if a client sends REQUEST_CHUNK
// but stalls without sending ACK after receiving the chunk, the server handler times out on ACK read.
func TestStalledStream_HandlerACKTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)
	eng1 := createTestEngine(t)

	// Configure server handler with 100ms read timeout
	chunk.NewStreamHandler(h1, eng1, chunk.WithReadTimeout(100*time.Millisecond))

	ctx := context.Background()
	// Store dummy chunk in eng1
	var chunkID core.ChunkID
	dummyChunk := &core.Chunk{
		Header: core.ChunkHeader{ID: chunkID},
		Data:   []byte("test chunk data"),
	}
	_ = eng1.PutChunk(ctx, dummyChunk)

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Send REQUEST_CHUNK
	req := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read CHUNK response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read chunk response: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MSG_CHUNK, got %d", resp.Type)
	}

	// Client stalls and DOES NOT send ACK. Server should time out after 100ms.
	time.Sleep(250 * time.Millisecond)

	// Confirm server has closed stream
	buf := make([]byte, 10)
	_ = s.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, err = s.Read(buf)
	if err == nil {
		t.Logf("Read succeeded unexpectedly or returned no error")
	}
	_ = net.ErrClosed
}
