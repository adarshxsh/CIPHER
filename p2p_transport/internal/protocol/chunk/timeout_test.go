package chunk_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestChunkProtocol_StalledServerReadTimeout(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to create peer 1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to create peer 2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	// Server registers handler that reads request but never responds (stalls)
	serverDone := make(chan struct{})
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		ws := transport.WrapStream(s)
		defer ws.Close()
		defer close(serverDone)

		// Read the request message
		_, _ = chunk.ReadMessage(ws)
		// Stall indefinitely without sending response
		buf := make([]byte, 10)
		_, _ = ws.Read(buf)
	})

	eng2 := createTestEngine(t)
	tr := transport.NewTransport(h2)

	// Set context with short timeout for client
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	client, err := chunk.NewClient(ctx, tr, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var id core.ContentID
	start := time.Now()
	_, err = client.Resolve(ctx, id)
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("Expected Resolve to fail due to timeout")
	}

	if duration > 1*time.Second {
		t.Errorf("Resolve took too long to fail: %v", duration)
	}
}

func TestChunkProtocol_StalledClientHandlerTimeout(t *testing.T) {
	mn := mocknet.New()
	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to create peer 1: %v", err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("Failed to create peer 2: %v", err)
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatalf("Failed to link peers: %v", err)
	}
	if err := mn.ConnectAllButSelf(); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	eng1 := createTestEngine(t)

	// Set up server handler on h1
	chunk.NewStreamHandler(h1, eng1)

	goroutinesBefore := runtime.NumGoroutine()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s, err := h2.NewStream(context.Background(), h1.ID(), protocol.ChunkTransportProtocolID)
		if err != nil {
			t.Errorf("Failed to open stream: %v", err)
			return
		}
		defer s.Close()

		ws := transport.WrapStream(s)
		ws.SetTimeout(100 * time.Millisecond)

		// Send valid REQUEST_MANIFEST message
		var badID core.ContentID
		req := chunk.BuildRequestManifest(badID)
		if err := chunk.WriteMessage(ws, req); err != nil {
			t.Errorf("Failed to send request: %v", err)
			return
		}

		// Client now stalls (does not read server response or close stream)
		time.Sleep(300 * time.Millisecond)
	}()

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	if goroutinesAfter > goroutinesBefore+2 {
		t.Errorf("Leaked handler goroutines: before %d, after %d", goroutinesBefore, goroutinesAfter)
	}
}
