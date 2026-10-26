package transport

import (
	"context"
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

// StreamPolicy defines timeout configurations for network streams.
type StreamPolicy struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// DefaultStreamPolicy returns sensible default timeout configurations (30s read/write, 60s idle).
var DefaultStreamPolicy = StreamPolicy{
	ReadTimeout:  30 * time.Second,
	WriteTimeout: 30 * time.Second,
	IdleTimeout:  60 * time.Second,
}

// TimeoutStream wraps network.Stream to enforce read, write, and idle timeouts automatically on I/O calls.
type TimeoutStream struct {
	network.Stream
	policy StreamPolicy
}

// NewTimeoutStream creates a new TimeoutStream decorator wrapping the provided network.Stream.
func NewTimeoutStream(s network.Stream, policy StreamPolicy) *TimeoutStream {
	if policy.ReadTimeout <= 0 && policy.IdleTimeout <= 0 {
		policy.ReadTimeout = DefaultStreamPolicy.ReadTimeout
		policy.IdleTimeout = DefaultStreamPolicy.IdleTimeout
	}
	if policy.WriteTimeout <= 0 && policy.IdleTimeout <= 0 {
		policy.WriteTimeout = DefaultStreamPolicy.WriteTimeout
		policy.IdleTimeout = DefaultStreamPolicy.IdleTimeout
	}
	ts := &TimeoutStream{
		Stream: s,
		policy: policy,
	}
	ts.refreshReadDeadline()
	ts.refreshWriteDeadline()
	return ts
}

// Policy returns the stream's configured timeout policy.
func (ts *TimeoutStream) Policy() StreamPolicy {
	return ts.policy
}

func (ts *TimeoutStream) refreshReadDeadline() {
	timeout := ts.policy.ReadTimeout
	if timeout <= 0 || (ts.policy.IdleTimeout > 0 && ts.policy.IdleTimeout < timeout) {
		timeout = ts.policy.IdleTimeout
	}
	if timeout > 0 {
		_ = ts.Stream.SetReadDeadline(time.Now().Add(timeout))
	}
}

func (ts *TimeoutStream) refreshWriteDeadline() {
	timeout := ts.policy.WriteTimeout
	if timeout <= 0 || (ts.policy.IdleTimeout > 0 && ts.policy.IdleTimeout < timeout) {
		timeout = ts.policy.IdleTimeout
	}
	if timeout > 0 {
		_ = ts.Stream.SetWriteDeadline(time.Now().Add(timeout))
	}
}

func (ts *TimeoutStream) Read(p []byte) (int, error) {
	ts.refreshReadDeadline()
	return ts.Stream.Read(p)
}

func (ts *TimeoutStream) Write(p []byte) (int, error) {
	ts.refreshWriteDeadline()
	return ts.Stream.Write(p)
}

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	SetupStreamHandlerWithPolicy(h, DefaultStreamPolicy)
}

// SetupStreamHandlerWithPolicy configures the host to handle incoming streams with a custom timeout policy.
func SetupStreamHandlerWithPolicy(h host.Host, policy StreamPolicy) {
	h.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		ts := NewTimeoutStream(s, policy)
		if err := transfer.Receive(ts); err != nil {
			log.Printf("Error receiving file: %v", err)
		}
	})
}

// Transport wraps the libp2p host to provide a simpler abstraction for connection and stream management.
type Transport struct {
	host   host.Host
	policy StreamPolicy
}

// NewTransport creates a new Transport abstraction with default timeout policy.
func NewTransport(h host.Host) *Transport {
	return &Transport{
		host:   h,
		policy: DefaultStreamPolicy,
	}
}

// NewTransportWithPolicy creates a new Transport abstraction with a custom timeout policy.
func NewTransportWithPolicy(h host.Host, policy StreamPolicy) *Transport {
	return &Transport{
		host:   h,
		policy: policy,
	}
}

// SetPolicy updates the transport timeout policy.
func (t *Transport) SetPolicy(policy StreamPolicy) {
	t.policy = policy
}

// Policy returns the transport's configured timeout policy.
func (t *Transport) Policy() StreamPolicy {
	return t.policy
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

	return NewTimeoutStream(s, t.policy), nil
}
