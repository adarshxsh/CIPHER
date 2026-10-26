package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
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
	DefaultIdleTimeout  = 30 * time.Second
	DefaultWriteTimeout = 30 * time.Second
)

// ManagedStream wraps a network.Stream to enforce default and configurable read/write deadlines.
type ManagedStream struct {
	network.Stream

	mu                  sync.RWMutex
	idleTimeout         time.Duration
	writeTimeout        time.Duration
	hasCustomRead       bool
	hasCustomWrite      bool
	customReadDeadline  time.Time
	customWriteDeadline time.Time
}

type ManagedStreamOption func(*ManagedStream)

func WithIdleTimeout(d time.Duration) ManagedStreamOption {
	return func(ms *ManagedStream) {
		ms.idleTimeout = d
	}
}

func WithReadTimeout(d time.Duration) ManagedStreamOption {
	return WithIdleTimeout(d)
}

func WithWriteTimeout(d time.Duration) ManagedStreamOption {
	return func(ms *ManagedStream) {
		ms.writeTimeout = d
	}
}

// NewManagedStream creates a ManagedStream decorator wrapping the given network.Stream.
func NewManagedStream(s network.Stream, opts ...ManagedStreamOption) *ManagedStream {
	ms := &ManagedStream{
		Stream:       s,
		idleTimeout:  DefaultIdleTimeout,
		writeTimeout: DefaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(ms)
	}

	if ms.Stream != nil {
		if ms.idleTimeout > 0 {
			_ = s.SetReadDeadline(time.Now().Add(ms.idleTimeout))
		}
		if ms.writeTimeout > 0 {
			_ = s.SetWriteDeadline(time.Now().Add(ms.writeTimeout))
		}
	}

	return ms
}

func (ms *ManagedStream) Read(p []byte) (int, error) {
	ms.mu.RLock()
	hasCustom := ms.hasCustomRead
	timeout := ms.idleTimeout
	ms.mu.RUnlock()

	if !hasCustom && timeout > 0 {
		_ = ms.Stream.SetReadDeadline(time.Now().Add(timeout))
	}
	return ms.Stream.Read(p)
}

func (ms *ManagedStream) Write(p []byte) (int, error) {
	ms.mu.RLock()
	hasCustom := ms.hasCustomWrite
	timeout := ms.writeTimeout
	ms.mu.RUnlock()

	if !hasCustom && timeout > 0 {
		_ = ms.Stream.SetWriteDeadline(time.Now().Add(timeout))
	}
	return ms.Stream.Write(p)
}

func (ms *ManagedStream) SetReadDeadline(t time.Time) error {
	ms.mu.Lock()
	ms.hasCustomRead = true
	ms.customReadDeadline = t
	ms.mu.Unlock()
	return ms.Stream.SetReadDeadline(t)
}

func (ms *ManagedStream) SetWriteDeadline(t time.Time) error {
	ms.mu.Lock()
	ms.hasCustomWrite = true
	ms.customWriteDeadline = t
	ms.mu.Unlock()
	return ms.Stream.SetWriteDeadline(t)
}

func (ms *ManagedStream) SetDeadline(t time.Time) error {
	ms.mu.Lock()
	ms.hasCustomRead = true
	ms.customReadDeadline = t
	ms.hasCustomWrite = true
	ms.customWriteDeadline = t
	ms.mu.Unlock()
	return ms.Stream.SetDeadline(t)
}

func (ms *ManagedStream) SetIdleTimeout(d time.Duration) {
	ms.mu.Lock()
	ms.idleTimeout = d
	ms.hasCustomRead = false
	ms.mu.Unlock()

	if ms.Stream != nil {
		if d > 0 {
			_ = ms.Stream.SetReadDeadline(time.Now().Add(d))
		} else {
			_ = ms.Stream.SetReadDeadline(time.Time{})
		}
	}
}

func (ms *ManagedStream) SetReadTimeout(d time.Duration) {
	ms.SetIdleTimeout(d)
}

func (ms *ManagedStream) SetWriteTimeout(d time.Duration) {
	ms.mu.Lock()
	ms.writeTimeout = d
	ms.hasCustomWrite = false
	ms.mu.Unlock()

	if ms.Stream != nil {
		if d > 0 {
			_ = ms.Stream.SetWriteDeadline(time.Now().Add(d))
		} else {
			_ = ms.Stream.SetWriteDeadline(time.Time{})
		}
	}
}

func (ms *ManagedStream) IdleTimeout() time.Duration {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.idleTimeout
}

func (ms *ManagedStream) WriteTimeout() time.Duration {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.writeTimeout
}

func (ms *ManagedStream) Close() error {
	_ = ms.Stream.SetDeadline(time.Time{})
	return ms.Stream.Close()
}

func (ms *ManagedStream) Reset() error {
	_ = ms.Stream.SetDeadline(time.Time{})
	return ms.Stream.Reset()
}

// WrapStreamHandler wraps a network.StreamHandler so incoming streams are wrapped in ManagedStream.
func WrapStreamHandler(handler network.StreamHandler, opts ...ManagedStreamOption) network.StreamHandler {
	return func(s network.Stream) {
		ms := NewManagedStream(s, opts...)
		handler(ms)
	}
}

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	h.SetStreamHandler(protocol.FileTransferProtocolID, WrapStreamHandler(func(s network.Stream) {
		if err := transfer.Receive(s); err != nil {
			log.Printf("Error receiving file: %v", err)
		}
	}))
}

// Transport wraps the libp2p host to provide a simpler abstraction for connection and stream management.
type Transport struct {
	host         host.Host
	idleTimeout  time.Duration
	writeTimeout time.Duration
}

type TransportOption func(*Transport)

func WithTransportIdleTimeout(d time.Duration) TransportOption {
	return func(t *Transport) {
		t.idleTimeout = d
	}
}

func WithTransportWriteTimeout(d time.Duration) TransportOption {
	return func(t *Transport) {
		t.writeTimeout = d
	}
}

// NewTransport creates a new Transport abstraction.
func NewTransport(h host.Host, opts ...TransportOption) *Transport {
	t := &Transport{
		host:         h,
		idleTimeout:  DefaultIdleTimeout,
		writeTimeout: DefaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Transport) SetIdleTimeout(d time.Duration) {
	t.idleTimeout = d
}

func (t *Transport) SetWriteTimeout(d time.Duration) {
	t.writeTimeout = d
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

// ConnectPeer directly connects using addrInfo
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

	return NewManagedStream(s, WithIdleTimeout(t.idleTimeout), WithWriteTimeout(t.writeTimeout)), nil
}
