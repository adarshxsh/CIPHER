package transport_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/transport"
)

func setupRealLoopbackNetwork(t testing.TB) (host.Host, host.Host) {
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

func TestStreamTimeouts(t *testing.T) {
	h1, h2 := setupRealLoopbackNetwork(t)

	var wg sync.WaitGroup
	wg.Add(1)

	h1.SetStreamHandler("/test/1.0.0", func(s network.Stream) {
		defer wg.Done()
		defer s.Reset()
		// Server handler: sets read deadline, reads nothing, expect timeout
		if err := transport.SetReadTimeout(s, 100*time.Millisecond); err != nil {
			t.Errorf("SetReadTimeout failed: %v", err)
			return
		}
		buf := make([]byte, 10)
		_, err := s.Read(buf)
		if err == nil {
			t.Errorf("Expected read timeout error, got nil")
			return
		}
		if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
			t.Errorf("Expected timeout error, got: %v", err)
		}
	})

	s, err := h2.NewStream(context.Background(), h1.ID(), "/test/1.0.0")
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}
	defer s.Close()

	// Wait for server stream handler to hit deadline and complete
	wg.Wait()

	// Verify SetWriteTimeout, SetTimeout, and ClearDeadlines helper functions
	if err := transport.SetWriteTimeout(s, 100*time.Millisecond); err != nil {
		t.Errorf("SetWriteTimeout failed: %v", err)
	}
	if err := transport.SetTimeout(s, 100*time.Millisecond); err != nil {
		t.Errorf("SetTimeout failed: %v", err)
	}
	if err := transport.ClearDeadlines(s); err != nil {
		t.Errorf("ClearDeadlines failed: %v", err)
	}
}
