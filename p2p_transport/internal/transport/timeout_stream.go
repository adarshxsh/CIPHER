package transport

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
)

// DefaultTimeout is the default duration for read, write, and idle timeouts.
const DefaultTimeout = 30 * time.Second

// TimeoutStream decorates a libp2p network.Stream to enforce and refresh read and write deadlines automatically.
type TimeoutStream struct {
	network.Stream
	readTimeoutNs  atomic.Int64
	writeTimeoutNs atomic.Int64
}

// TimeoutOption represents a functional option for configuring TimeoutStream.
type TimeoutOption func(*TimeoutStream)

// WithReadTimeout sets the read timeout duration.
func WithReadTimeout(d time.Duration) TimeoutOption {
	return func(ts *TimeoutStream) {
		ts.readTimeoutNs.Store(int64(d))
	}
}

// WithWriteTimeout sets the write timeout duration.
func WithWriteTimeout(d time.Duration) TimeoutOption {
	return func(ts *TimeoutStream) {
		ts.writeTimeoutNs.Store(int64(d))
	}
}

// WithIdleTimeout sets both the read and write timeout durations.
func WithIdleTimeout(d time.Duration) TimeoutOption {
	return func(ts *TimeoutStream) {
		ts.readTimeoutNs.Store(int64(d))
		ts.writeTimeoutNs.Store(int64(d))
	}
}

// NewTimeoutStream creates a new TimeoutStream decorating the given network.Stream.
func NewTimeoutStream(s network.Stream, opts ...TimeoutOption) *TimeoutStream {
	if ts, ok := s.(*TimeoutStream); ok {
		for _, opt := range opts {
			opt(ts)
		}
		return ts
	}

	ts := &TimeoutStream{
		Stream: s,
	}
	ts.readTimeoutNs.Store(int64(DefaultTimeout))
	ts.writeTimeoutNs.Store(int64(DefaultTimeout))

	for _, opt := range opts {
		opt(ts)
	}

	rTimeout := ts.ReadTimeout()
	if rTimeout > 0 {
		_ = s.SetReadDeadline(time.Now().Add(rTimeout))
	}
	wTimeout := ts.WriteTimeout()
	if wTimeout > 0 {
		_ = s.SetWriteDeadline(time.Now().Add(wTimeout))
	}

	return ts
}

// Read TimeoutStream implementation: refreshes read deadline and executes Read.
func (ts *TimeoutStream) Read(p []byte) (n int, err error) {
	rTimeout := ts.ReadTimeout()
	if rTimeout > 0 {
		if setErr := ts.Stream.SetReadDeadline(time.Now().Add(rTimeout)); setErr != nil {
			return 0, setErr
		}
	}

	n, err = ts.Stream.Read(p)
	if err != nil && isTimeoutError(err) {
		_ = ts.Stream.Reset()
	}
	return n, err
}

// Write TimeoutStream implementation: refreshes write deadline and executes Write.
func (ts *TimeoutStream) Write(p []byte) (n int, err error) {
	wTimeout := ts.WriteTimeout()
	if wTimeout > 0 {
		if setErr := ts.Stream.SetWriteDeadline(time.Now().Add(wTimeout)); setErr != nil {
			return 0, setErr
		}
	}

	n, err = ts.Stream.Write(p)
	if err != nil && isTimeoutError(err) {
		_ = ts.Stream.Reset()
	}
	return n, err
}

// SetReadDeadline sets the read deadline on the underlying stream and updates the internal read timeout duration.
func (ts *TimeoutStream) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		ts.readTimeoutNs.Store(0)
	} else {
		d := time.Until(t)
		if d > 0 {
			ts.readTimeoutNs.Store(int64(d))
		}
	}
	return ts.Stream.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline on the underlying stream and updates the internal write timeout duration.
func (ts *TimeoutStream) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		ts.writeTimeoutNs.Store(0)
	} else {
		d := time.Until(t)
		if d > 0 {
			ts.writeTimeoutNs.Store(int64(d))
		}
	}
	return ts.Stream.SetWriteDeadline(t)
}

// SetDeadline sets both read and write deadlines on the underlying stream.
func (ts *TimeoutStream) SetDeadline(t time.Time) error {
	errR := ts.SetReadDeadline(t)
	errW := ts.SetWriteDeadline(t)
	if errR != nil {
		return errR
	}
	return errW
}

// SetReadTimeout dynamically updates the read timeout duration.
func (ts *TimeoutStream) SetReadTimeout(d time.Duration) {
	ts.readTimeoutNs.Store(int64(d))
	if d > 0 {
		_ = ts.Stream.SetReadDeadline(time.Now().Add(d))
	} else {
		_ = ts.Stream.SetReadDeadline(time.Time{})
	}
}

// SetWriteTimeout dynamically updates the write timeout duration.
func (ts *TimeoutStream) SetWriteTimeout(d time.Duration) {
	ts.writeTimeoutNs.Store(int64(d))
	if d > 0 {
		_ = ts.Stream.SetWriteDeadline(time.Now().Add(d))
	} else {
		_ = ts.Stream.SetWriteDeadline(time.Time{})
	}
}

// ReadTimeout returns the current read timeout duration.
func (ts *TimeoutStream) ReadTimeout() time.Duration {
	return time.Duration(ts.readTimeoutNs.Load())
}

// WriteTimeout returns the current write timeout duration.
func (ts *TimeoutStream) WriteTimeout() time.Duration {
	return time.Duration(ts.writeTimeoutNs.Load())
}

// isTimeoutError returns true if the error represents a deadline timeout or context cancellation/timeout.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "deadline exceeded") || strings.Contains(errStr, "i/o timeout")
}
