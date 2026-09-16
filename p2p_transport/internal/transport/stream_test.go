package transport

import (
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
)

type deadlineMockStream struct {
	network.Stream
	mu                  sync.Mutex
	readDeadlineCalled  bool
	writeDeadlineCalled bool
	lastReadDeadline    time.Time
	lastWriteDeadline   time.Time
}

func (m *deadlineMockStream) SetReadDeadline(t time.Time) error {
	m.mu.Lock()
	m.readDeadlineCalled = true
	m.lastReadDeadline = t
	m.mu.Unlock()
	return nil
}

func (m *deadlineMockStream) SetWriteDeadline(t time.Time) error {
	m.mu.Lock()
	m.writeDeadlineCalled = true
	m.lastWriteDeadline = t
	m.mu.Unlock()
	return nil
}

func (m *deadlineMockStream) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (m *deadlineMockStream) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestStream_DeadlinesCalledBeforeReadWrite(t *testing.T) {
	mock := &deadlineMockStream{}
	ctx := context.Background()
	ws := WrapStreamWithContext(ctx, mock)
	ws.SetTimeout(5 * time.Second)

	beforeRead := time.Now()
	buf := make([]byte, 10)
	_, _ = ws.Read(buf)

	mock.mu.Lock()
	if !mock.readDeadlineCalled {
		t.Errorf("Expected SetReadDeadline to be called before Read")
	}
	if mock.lastReadDeadline.Before(beforeRead.Add(4 * time.Second)) {
		t.Errorf("Expected read deadline to be set in future, got %v", mock.lastReadDeadline)
	}
	mock.mu.Unlock()

	beforeWrite := time.Now()
	_, _ = ws.Write([]byte("hello"))

	mock.mu.Lock()
	if !mock.writeDeadlineCalled {
		t.Errorf("Expected SetWriteDeadline to be called before Write")
	}
	if mock.lastWriteDeadline.Before(beforeWrite.Add(4 * time.Second)) {
		t.Errorf("Expected write deadline to be set in future, got %v", mock.lastWriteDeadline)
	}
	mock.mu.Unlock()
}

func TestStream_ContextDeadlineInherited(t *testing.T) {
	mock := &deadlineMockStream{}
	ctxDeadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), ctxDeadline)
	defer cancel()

	ws := WrapStreamWithContext(ctx, mock)
	ws.SetTimeout(30 * time.Second) // Longer than context deadline

	buf := make([]byte, 10)
	_, _ = ws.Read(buf)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if !mock.lastReadDeadline.Equal(ctxDeadline) {
		t.Errorf("Expected deadline to match context deadline %v, got %v", ctxDeadline, mock.lastReadDeadline)
	}
}

func TestStream_HungConnectionTimesOutAndCloses(t *testing.T) {
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

	testPID := protocol.ID("/test/timeout/1.0.0")

	handlerDone := make(chan struct{})
	h1.SetStreamHandler(testPID, func(s network.Stream) {
		ws := WrapStream(s)
		ws.SetTimeout(100 * time.Millisecond)
		defer ws.Close()
		defer close(handlerDone)

		buf := make([]byte, 100)
		n, err := ws.Read(buf)
		if err != nil || n != 1 {
			t.Errorf("Expected 1 byte initial read, got n=%d err=%v", n, err)
			return
		}
		// Second read blocks and times out on stalled peer
		_, err = ws.Read(buf)
		if err == nil {
			t.Errorf("Expected read timeout error, got nil")
		}
	})

	s, err := h2.NewStream(context.Background(), h1.ID(), testPID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Write 1 byte to trigger stream negotiation and handler invocation on h1, then stall
	_, _ = s.Write([]byte{0x01})

	// Peer 2 does not send any data (simulating hung remote peer)
	select {
	case <-handlerDone:
		// Stream handler finished cleanly after timing out
	case <-time.After(2 * time.Second):
		t.Fatalf("Stream handler did not time out within expected duration")
	}
}

func TestStream_ZeroLeakedGoroutinesOnTimeout(t *testing.T) {
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

	testPID := protocol.ID("/test/goroutine-leak/1.0.0")

	var wg sync.WaitGroup
	h1.SetStreamHandler(testPID, func(s network.Stream) {
		ws := WrapStream(s)
		ws.SetTimeout(50 * time.Millisecond)
		defer ws.Close()
		defer wg.Done()

		buf := make([]byte, 100)
		_, _ = ws.Read(buf) // read initial 0x01
		_, _ = ws.Read(buf) // block and time out
	})

	goroutinesBefore := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		s, err := h2.NewStream(context.Background(), h1.ID(), testPID)
		if err != nil {
			t.Fatalf("Failed to open stream %d: %v", i, err)
		}
		_, _ = s.Write([]byte{0x01})
		// Keep client side stream open for a short time then close it
		go func(str network.Stream) {
			time.Sleep(100 * time.Millisecond)
			_ = str.Close()
		}(s)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	// Allow small tolerance for runtime background tasks if any, but handler goroutines must exit
	if goroutinesAfter > goroutinesBefore+2 {
		t.Errorf("Possible goroutine leak: before %d, after %d", goroutinesBefore, goroutinesAfter)
	}
}
