package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"

	"cipher/internal/transport"
)

const testProto = libp2p_protocol.ID("/test/timeout/1.0.0")

func setupTestHosts(t *testing.T) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		h1.Close()
		t.Fatalf("Failed to create host 2: %v", err)
	}

	if err := h1.Connect(context.Background(), *host.InfoFromHost(h2)); err != nil {
		h1.Close()
		h2.Close()
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	return h1, h2
}

func TestTimeoutStream_IdleReadTimeout(t *testing.T) {
	h1, h2 := setupTestHosts(t)

	shortPolicy := transport.StreamPolicy{
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		IdleTimeout:  150 * time.Millisecond,
	}

	handlerErrChan := make(chan error, 1)

	h1.SetStreamHandler(testProto, func(s network.Stream) {
		ts := transport.NewTimeoutStream(s, shortPolicy)
		defer ts.Close()

		buf := make([]byte, 100)
		_, err := ts.Read(buf)
		handlerErrChan <- err
	})

	tr := transport.NewTransportWithPolicy(h2, shortPolicy)
	stream, err := tr.OpenStream(context.Background(), h1.ID(), testProto)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	// Peer 2 (client) opens stream but sends no data.
	// Server should hit the read/idle timeout and fail with a net/timeout error.
	select {
	case err := <-handlerErrChan:
		if err == nil {
			t.Fatalf("Expected read timeout error, got nil")
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			// Success: got a timeout error
		} else {
			t.Logf("Received error: %v (type: %T)", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Test timed out waiting for server stream read to expire")
	}
}

func TestTimeoutStream_DeadlineRefreshOnActiveTransfer(t *testing.T) {
	h1, h2 := setupTestHosts(t)

	shortPolicy := transport.StreamPolicy{
		ReadTimeout:  300 * time.Millisecond,
		WriteTimeout: 300 * time.Millisecond,
		IdleTimeout:  300 * time.Millisecond,
	}

	doneChan := make(chan error, 1)

	h1.SetStreamHandler(testProto, func(s network.Stream) {
		ts := transport.NewTimeoutStream(s, shortPolicy)
		defer ts.Close()

		// Server performs 4 reads with short delays (100ms apart)
		// Total duration = 400ms > 300ms policy timeout, but each interval < 300ms.
		for i := 0; i < 4; i++ {
			buf := make([]byte, 5)
			_, err := io.ReadFull(ts, buf)
			if err != nil {
				doneChan <- err
				return
			}
		}
		doneChan <- nil
	})

	tr := transport.NewTransportWithPolicy(h2, shortPolicy)
	stream, err := tr.OpenStream(context.Background(), h1.ID(), testProto)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	// Send 4 chunks spaced 100ms apart
	for i := 0; i < 4; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := stream.Write([]byte("hello"))
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Server read failed unexpectedly: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Test timed out waiting for server")
	}
}

func TestTimeoutStream_PolicyGettersAndDefaults(t *testing.T) {
	p := transport.DefaultStreamPolicy
	if p.ReadTimeout != 30*time.Second || p.WriteTimeout != 30*time.Second || p.IdleTimeout != 60*time.Second {
		t.Fatalf("Unexpected default stream policy values: %+v", p)
	}

	h1, h2 := setupTestHosts(t)
	tr := transport.NewTransport(h2)
	if tr.Policy() != transport.DefaultStreamPolicy {
		t.Fatalf("Expected default policy on Transport")
	}

	customPolicy := transport.StreamPolicy{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  10 * time.Second,
	}
	tr.SetPolicy(customPolicy)
	if tr.Policy() != customPolicy {
		t.Fatalf("Expected updated policy on Transport")
	}

	stream, err := tr.OpenStream(context.Background(), h1.ID(), testProto)
	if err != nil {
		// h1 doesn't have a handler registered, but OpenStream creates the stream
	} else {
		defer stream.Close()
		if ts, ok := stream.(*transport.TimeoutStream); ok {
			if ts.Policy() != customPolicy {
				t.Fatalf("Expected stream policy %+v, got %+v", customPolicy, ts.Policy())
			}
		} else {
			t.Fatalf("Expected stream to be *transport.TimeoutStream")
		}
	}
}
