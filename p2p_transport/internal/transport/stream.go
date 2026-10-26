package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

const (
	// DefaultConnectTimeout is the default timeout applied to peer connection dials.
	DefaultConnectTimeout = 15 * time.Second
	// DefaultStreamTimeout is the default deadline applied to new transport streams.
	DefaultStreamTimeout = 15 * time.Second
)

var (
	// ErrInvalidPeerID is returned when an empty peer ID is provided.
	ErrInvalidPeerID = errors.New("invalid peer ID: peer ID cannot be empty")
	// ErrInvalidProtocol is returned when an empty protocol ID is provided.
	ErrInvalidProtocol = errors.New("invalid protocol ID: protocol ID cannot be empty")
)

// Option defines a functional option for configuring Transport.
type Option func(*Transport)

// WithConnectTimeout sets a custom connection dial timeout for Transport.
func WithConnectTimeout(d time.Duration) Option {
	return func(t *Transport) {
		t.connectTimeout = d
	}
}

// WithStreamTimeout sets a custom stream deadline timeout for Transport.
func WithStreamTimeout(d time.Duration) Option {
	return func(t *Transport) {
		t.streamTimeout = d
	}
}

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	SetupStreamHandlerWithTimeout(h, DefaultStreamTimeout)
}

// SetupStreamHandlerWithTimeout configures the host to handle incoming streams with a strict deadline timeout.
func SetupStreamHandlerWithTimeout(h host.Host, timeout time.Duration) {
	h.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		if timeout > 0 {
			if err := s.SetDeadline(time.Now().Add(timeout)); err != nil {
				log.Printf("Failed to set stream deadline: %v", err)
				_ = s.Reset()
				return
			}
		}
		if err := transfer.Receive(s); err != nil {
			log.Printf("Error receiving file: %v", err)
			_ = s.Reset()
			return
		}
	})
}

// Transport wraps the libp2p host to provide a simpler abstraction for connection and stream management.
type Transport struct {
	host           host.Host
	connectTimeout time.Duration
	streamTimeout  time.Duration
}

// NewTransport creates a new Transport abstraction.
func NewTransport(h host.Host, opts ...Option) *Transport {
	t := &Transport{
		host:           h,
		connectTimeout: DefaultConnectTimeout,
		streamTimeout:  DefaultStreamTimeout,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Host returns the underlying libp2p host.
func (t *Transport) Host() host.Host {
	return t.host
}

// Connect dials the target peer and establishes the initial connection (likely a relayed connection).
func (t *Transport) Connect(ctx context.Context, target string) (*peer.AddrInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error before connect: %w", err)
	}

	maddr, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		return nil, fmt.Errorf("invalid multiaddress: %w", err)
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract peer info: %w", err)
	}

	timeout := t.connectTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
	defer dialCancel()

	if err := t.host.Connect(dialCtx, *addrInfo); err != nil {
		return nil, fmt.Errorf("h.Connect failed: %w", err)
	}

	return addrInfo, nil
}

// ConnectPeer directly connects to a peer using its AddrInfo.
func (t *Transport) ConnectPeer(ctx context.Context, addrInfo peer.AddrInfo) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context error before connect: %w", err)
	}

	if addrInfo.ID == "" {
		return ErrInvalidPeerID
	}

	timeout := t.connectTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
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

// OpenStream opens a stream to the target peer for the specified protocol ID, enforcing context timeouts and read/write deadlines.
func (t *Transport) OpenStream(ctx context.Context, target peer.ID, pid libp2p_protocol.ID) (network.Stream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context error before opening stream: %w", err)
	}

	if target == "" {
		return nil, ErrInvalidPeerID
	}
	if pid == "" {
		return nil, ErrInvalidProtocol
	}

	timeout := t.streamTimeout
	if timeout <= 0 {
		timeout = DefaultStreamTimeout
	}

	var streamCtx context.Context
	var streamCancel context.CancelFunc

	if deadline, ok := ctx.Deadline(); ok {
		streamCtx, streamCancel = context.WithDeadline(ctx, deadline)
	} else {
		streamCtx, streamCancel = context.WithTimeout(ctx, timeout)
	}
	defer streamCancel()

	// WithAllowLimitedConn serves as a fallback. If a direct connection (from DCUtR) is available,
	// libp2p will prefer it. If not, the application stream can still flow over the limited relay connection.
	streamCtx = network.WithAllowLimitedConn(streamCtx, "file-transfer-relay")

	s, err := t.host.NewStream(streamCtx, target, pid)
	if err != nil {
		return nil, fmt.Errorf("NewStream failed: %w", err)
	}

	// Apply strict deadline to underlying stream
	if deadline, ok := ctx.Deadline(); ok {
		if err := s.SetDeadline(deadline); err != nil {
			_ = s.Reset()
			return nil, fmt.Errorf("failed to set stream deadline: %w", err)
		}
	} else if timeout > 0 {
		if err := s.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = s.Reset()
			return nil, fmt.Errorf("failed to set stream deadline: %w", err)
		}
	}

	return s, nil
}

// SetStreamDeadline sets read and write deadlines on an active stream.
func (t *Transport) SetStreamDeadline(s network.Stream, timeout time.Duration) error {
	if s == nil {
		return fmt.Errorf("stream is nil")
	}
	if timeout <= 0 {
		return s.SetDeadline(time.Time{})
	}
	return s.SetDeadline(time.Now().Add(timeout))
}

// SetStreamReadDeadline sets the read deadline on an active stream.
func (t *Transport) SetStreamReadDeadline(s network.Stream, timeout time.Duration) error {
	if s == nil {
		return fmt.Errorf("stream is nil")
	}
	if timeout <= 0 {
		return s.SetReadDeadline(time.Time{})
	}
	return s.SetReadDeadline(time.Now().Add(timeout))
}

// SetStreamWriteDeadline sets the write deadline on an active stream.
func (t *Transport) SetStreamWriteDeadline(s network.Stream, timeout time.Duration) error {
	if s == nil {
		return fmt.Errorf("stream is nil")
	}
	if timeout <= 0 {
		return s.SetWriteDeadline(time.Time{})
	}
	return s.SetWriteDeadline(time.Now().Add(timeout))
}
