package transport

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

// mockStream implements network.Stream for unit testing.
type mockStream struct {
	readFunc         func(p []byte) (int, error)
	writeFunc        func(p []byte) (int, error)
	closeFunc        func() error
	resetFunc        func() error
	setReadDeadline  func(t time.Time) error
	setWriteDeadline func(t time.Time) error
	setDeadline      func(t time.Time) error

	readDeadline  atomic.Pointer[time.Time]
	writeDeadline atomic.Pointer[time.Time]
	resetCalled   atomic.Bool
	closeCalled   atomic.Bool
}

func newMockStream() *mockStream {
	return &mockStream{}
}

func (m *mockStream) Read(p []byte) (int, error) {
	if m.readFunc != nil {
		return m.readFunc(p)
	}
	return 0, io.EOF
}

func (m *mockStream) Write(p []byte) (int, error) {
	if m.writeFunc != nil {
		return m.writeFunc(p)
	}
	return len(p), nil
}

func (m *mockStream) Close() error {
	m.closeCalled.Store(true)
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockStream) CloseWrite() error { return nil }
func (m *mockStream) CloseRead() error  { return nil }

func (m *mockStream) Reset() error {
	m.resetCalled.Store(true)
	if m.resetFunc != nil {
		return m.resetFunc()
	}
	return nil
}

func (m *mockStream) ResetWithError(errCode network.StreamErrorCode) error {
	return m.Reset()
}

func (m *mockStream) SetDeadline(t time.Time) error {
	m.setDeadlineCall(t)
	if m.setDeadline != nil {
		return m.setDeadline(t)
	}
	return nil
}

func (m *mockStream) setDeadlineCall(t time.Time) {
	m.readDeadline.Store(&t)
	m.writeDeadline.Store(&t)
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	m.readDeadline.Store(&t)
	if m.setReadDeadline != nil {
		return m.setReadDeadline(t)
	}
	return nil
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	m.writeDeadline.Store(&t)
	if m.setWriteDeadline != nil {
		return m.setWriteDeadline(t)
	}
	return nil
}

func (m *mockStream) ID() string                           { return "mock-stream-id" }
func (m *mockStream) Protocol() libp2p_protocol.ID         { return "/mock/1.0.0" }
func (m *mockStream) SetProtocol(libp2p_protocol.ID) error { return nil }
func (m *mockStream) Stat() network.Stats                  { return network.Stats{} }
func (m *mockStream) Conn() network.Conn                   { return nil }
func (m *mockStream) Scope() network.StreamScope           { return nil }

func TestNewTimeoutStreamOptionsAndDefaults(t *testing.T) {
	m := newMockStream()
	ts := NewTimeoutStream(m, WithReadTimeout(10*time.Second), WithWriteTimeout(20*time.Second))

	if ts.ReadTimeout() != 10*time.Second {
		t.Errorf("expected read timeout 10s, got %v", ts.ReadTimeout())
	}
	if ts.WriteTimeout() != 20*time.Second {
		t.Errorf("expected write timeout 20s, got %v", ts.WriteTimeout())
	}

	ts2 := NewTimeoutStream(m, WithIdleTimeout(15*time.Second))
	if ts2.ReadTimeout() != 15*time.Second || ts2.WriteTimeout() != 15*time.Second {
		t.Errorf("expected idle timeout 15s, got read=%v write=%v", ts2.ReadTimeout(), ts2.WriteTimeout())
	}
}

func TestTimeoutStreamReadRefreshAndResetOnTimeout(t *testing.T) {
	m := newMockStream()
	m.readFunc = func(p []byte) (int, error) {
		return 0, context.DeadlineExceeded
	}

	ts := NewTimeoutStream(m, WithReadTimeout(50*time.Millisecond))

	buf := make([]byte, 10)
	_, err := ts.Read(buf)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded error, got %v", err)
	}
	if !m.resetCalled.Load() {
		t.Errorf("expected stream to be Reset on timeout error, but Reset was not called")
	}
}

func TestTimeoutStreamWriteRefreshAndResetOnTimeout(t *testing.T) {
	m := newMockStream()
	m.writeFunc = func(p []byte) (int, error) {
		return 0, context.DeadlineExceeded
	}

	ts := NewTimeoutStream(m, WithWriteTimeout(50*time.Millisecond))

	_, err := ts.Write([]byte("test"))

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded error, got %v", err)
	}
	if !m.resetCalled.Load() {
		t.Errorf("expected stream to be Reset on timeout error, but Reset was not called")
	}
}

func TestTimeoutStreamDynamicDeadlineExtension(t *testing.T) {
	m := newMockStream()
	ts := NewTimeoutStream(m)

	newDeadline := time.Now().Add(2 * time.Minute)
	if err := ts.SetReadDeadline(newDeadline); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	readDL := m.readDeadline.Load()
	if readDL == nil || !readDL.Equal(newDeadline) {
		t.Errorf("expected underlying read deadline %v, got %v", newDeadline, readDL)
	}

	if ts.ReadTimeout() <= 0 {
		t.Errorf("expected positive read timeout after extending deadline")
	}
}

func TestTimeoutStreamConcurrentReadWrite(t *testing.T) {
	m := newMockStream()
	m.readFunc = func(p []byte) (int, error) {
		return copy(p, "data"), nil
	}
	m.writeFunc = func(p []byte) (int, error) {
		return len(p), nil
	}

	ts := NewTimeoutStream(m, WithIdleTimeout(1*time.Second))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, 10)
			_, _ = ts.Read(buf)
		}()
		go func() {
			defer wg.Done()
			_, _ = ts.Write([]byte("ping"))
		}()
	}
	wg.Wait()
}

func TestWrapAlreadyWrappedStream(t *testing.T) {
	m := newMockStream()
	ts1 := NewTimeoutStream(m, WithReadTimeout(5*time.Second))
	ts2 := NewTimeoutStream(ts1, WithReadTimeout(10*time.Second))

	if ts1 != ts2 {
		t.Errorf("expected NewTimeoutStream on TimeoutStream to return same instance")
	}
	if ts2.ReadTimeout() != 10*time.Second {
		t.Errorf("expected updated read timeout 10s, got %v", ts2.ReadTimeout())
	}
}

// Ensure mockStream satisfies network.Stream interface
var _ network.Stream = (*mockStream)(nil)
var _ peer.ID = peer.ID("")
