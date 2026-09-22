package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

const DefaultStreamTimeout = 30 * time.Second

// Stream wraps network.Stream to enforce context-bounded read and write deadlines.
type Stream struct {
	network.Stream
	timeout time.Duration
	ctx     context.Context
}

// WrapStream wraps a libp2p network.Stream with standard timeout and context handling.
func WrapStream(s network.Stream) *Stream {
	if s == nil {
		return nil
	}
	if ws, ok := s.(*Stream); ok {
		return ws
	}
	return &Stream{
		Stream:  s,
		timeout: DefaultStreamTimeout,
		ctx:     context.Background(),
	}
}

// WrapStreamWithContext wraps a libp2p network.Stream bound to a parent context.
func WrapStreamWithContext(ctx context.Context, s network.Stream) *Stream {
	if s == nil {
		return nil
	}
	if ws, ok := s.(*Stream); ok {
		if ctx != nil {
			ws.ctx = ctx
		}
		return ws
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &Stream{
		Stream:  s,
		timeout: DefaultStreamTimeout,
		ctx:     ctx,
	}
}

func (s *Stream) SetTimeout(d time.Duration) {
	s.timeout = d
}

func (s *Stream) SetContext(ctx context.Context) {
	s.ctx = ctx
}

func (s *Stream) getDeadline() time.Time {
	deadline := time.Now().Add(s.timeout)
	if s.ctx != nil {
		if err := s.ctx.Err(); err != nil {
			return time.Now().Add(-1 * time.Second)
		}
		if d, ok := s.ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
	}
	return deadline
}

func (s *Stream) Read(p []byte) (int, error) {
	deadline := s.getDeadline()
	_ = s.Stream.SetReadDeadline(deadline)

	var timer *time.Timer
	if !deadline.IsZero() {
		dur := time.Until(deadline)
		if dur <= 0 {
			_ = s.Stream.SetReadDeadline(time.Now().Add(-1 * time.Second))
			_ = s.Stream.Reset()
			_ = s.Stream.Close()
			return 0, os.ErrDeadlineExceeded
		}
		timer = time.AfterFunc(dur, func() {
			_ = s.Stream.SetReadDeadline(time.Now().Add(-1 * time.Second))
			_ = s.Stream.Reset()
			_ = s.Stream.Close()
		})
	}

	n, err := s.Stream.Read(p)
	if timer != nil {
		timer.Stop()
	}

	if err != nil && isTimeoutErr(err) {
		_ = s.Stream.Reset()
		_ = s.Stream.Close()
	}
	return n, err
}

func (s *Stream) Write(p []byte) (int, error) {
	deadline := s.getDeadline()
	_ = s.Stream.SetWriteDeadline(deadline)

	var timer *time.Timer
	if !deadline.IsZero() {
		dur := time.Until(deadline)
		if dur <= 0 {
			_ = s.Stream.SetWriteDeadline(time.Now().Add(-1 * time.Second))
			_ = s.Stream.Reset()
			_ = s.Stream.Close()
			return 0, os.ErrDeadlineExceeded
		}
		timer = time.AfterFunc(dur, func() {
			_ = s.Stream.SetWriteDeadline(time.Now().Add(-1 * time.Second))
			_ = s.Stream.Reset()
			_ = s.Stream.Close()
		})
	}

	n, err := s.Stream.Write(p)
	if timer != nil {
		timer.Stop()
	}

	if err != nil && isTimeoutErr(err) {
		_ = s.Stream.Reset()
		_ = s.Stream.Close()
	}
	return n, err
}

func isTimeoutErr(err error) bool {
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
	errMsg := err.Error()
	return strings.Contains(errMsg, "deadline exceeded") || strings.Contains(errMsg, "i/o timeout")
}

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	h.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		ws := WrapStream(s)
		defer ws.Close()
		if err := transfer.Receive(ws); err != nil {
			log.Printf("Error receiving file: %v", err)
		}
	})
}

// Transport wraps the libp2p host to provide a simpler abstraction for connection and stream management.
type Transport struct {
	host host.Host
}

// NewTransport creates a new Transport abstraction.
func NewTransport(h host.Host) *Transport {
	return &Transport{host: h}
}

// Connect dials the target peer and establishes the initial connection (likely a relayed connection).
func (t *Transport) Connect(ctx context.Context, target string) (*peer.AddrInfo, error) {
	maddr, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		return nil, fmt.Errorf("invalid multiaddress: %w", err)
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract peer info: %w", err)
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	if err := t.host.Connect(dialCtx, *addrInfo); err != nil {
		return nil, fmt.Errorf("h.Connect failed: %w", err)
	}

	return addrInfo, nil
}

// This is actually the same func as above Connect, but just that this directly connects using addrInfo, while Connect uses a string
func (t *Transport) ConnectPeer(ctx context.Context, addrInfo peer.AddrInfo) error {
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	if err := t.host.Connect(dialCtx, addrInfo); err != nil {
		return fmt.Errorf(
			"failed to connect to peer %s: %w",
			addrInfo.ID,
			err,
		)
	}

	return nil
}

func (t *Transport) OpenStream(ctx context.Context, target peer.ID, pid libp2p_protocol.ID) (network.Stream, error) {
	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	// WithAllowLimitedConn serves as a fallback. If a direct connection (from DCUtR) is available,
	// libp2p will prefer it. If not, the application stream can still flow over the limited relay connection.
	streamCtx = network.WithAllowLimitedConn(streamCtx, "file-transfer-relay")

	s, err := t.host.NewStream(streamCtx, target, pid)
	if err != nil {
		return nil, fmt.Errorf("NewStream failed: %w", err)
	}

	return WrapStreamWithContext(ctx, s), nil
}
