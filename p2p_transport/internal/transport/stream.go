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

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	SetStreamHandler(h, protocol.FileTransferProtocolID, func(s network.Stream) {
		if err := transfer.Receive(s); err != nil {
			log.Printf("Error receiving file: %v", err)
		}
	})
}

// SetStreamHandler registers a stream handler on the host that automatically decorates incoming streams with TimeoutStream.
func SetStreamHandler(h host.Host, pid libp2p_protocol.ID, handler func(network.Stream), opts ...TimeoutOption) {
	h.SetStreamHandler(pid, func(s network.Stream) {
		ts := NewTimeoutStream(s, opts...)
		handler(ts)
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

// SetStreamHandler registers a stream handler on the transport's host that automatically decorates incoming streams with TimeoutStream.
func (t *Transport) SetStreamHandler(pid libp2p_protocol.ID, handler func(network.Stream), opts ...TimeoutOption) {
	SetStreamHandler(t.host, pid, handler, opts...)
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

	return NewTimeoutStream(s), nil
}
