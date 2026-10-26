package transport

import (
	"cipher/internal/discovery"
	"cipher/internal/protocol"
	"context"

	"fmt"
	"log"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool) (host.Host, *dht.IpfsDHT, error) {
	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	listenAddrs := []string{addr}
	if wsPort > 0 {
		wsAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsPort)
		listenAddrs = append(listenAddrs, wsAddr)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	limitConfig := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Conns:         1024,
			ConnsInbound:  1024,
			ConnsOutbound: 1024,
			Streams:         2048,
			StreamsInbound:  2048,
			StreamsOutbound: 2048,
			FD:            512,
			Memory:        256 * 1024 * 1024, // 256 MB
		},
		PeerDefault: rcmgr.ResourceLimits{
			Conns:         16,
			ConnsInbound:  16,
			ConnsOutbound: 16,
			Streams:         64,
			StreamsInbound:  64,
			StreamsOutbound: 64,
			Memory:        rcmgr.DefaultLimit64,
			FD:            rcmgr.DefaultLimit,
		},
		ProtocolDefault: rcmgr.ResourceLimits{
			Streams:         1024,
			StreamsInbound:  1024,
			StreamsOutbound: 1024,
			Memory:        128 * 1024 * 1024,
		},
		ProtocolPeerDefault: rcmgr.ResourceLimits{
			Streams:         64,
			StreamsInbound:  64,
			StreamsOutbound: 64,
			Memory:        32 * 1024 * 1024,
		},
		Protocol: map[libp2p_protocol.ID]rcmgr.ResourceLimits{
			protocol.ChunkTransportProtocolID: {
				Streams:         1024,
				StreamsInbound:  1024,
				StreamsOutbound: 1024,
				Memory:        128 * 1024 * 1024,
			},
			protocol.FileTransferProtocolID: {
				Streams:         512,
				StreamsInbound:  512,
				StreamsOutbound: 512,
				Memory:        64 * 1024 * 1024,
			},
		},
		ProtocolPeer: map[libp2p_protocol.ID]rcmgr.ResourceLimits{
			protocol.ChunkTransportProtocolID: {
				Streams:         64,
				StreamsInbound:  64,
				StreamsOutbound: 64,
				Memory:        32 * 1024 * 1024,
			},
			protocol.FileTransferProtocolID: {
				Streams:         32,
				StreamsInbound:  32,
				StreamsOutbound: 32,
				Memory:        16 * 1024 * 1024,
			},
		},
	}

	limiter := rcmgr.NewFixedLimiter(limitConfig.Build(scalingLimits.AutoScale()))
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
		libp2p.ResourceManager(rm),
	}

	if priv != nil {
		opts = append(opts, libp2p.Identity(priv))
	}

	if relayAddr != "" {
		maddr, err := multiaddr.NewMultiaddr(relayAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid relay address: %w", err)
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid relay peer info: %w", err)
		}
		opts = append(opts,
			libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*addrInfo}),
			libp2p.ForceReachabilityPrivate(), // Force Private to instantly enable Hole Punching!
		)
		if !forceRelay {
			opts = append(opts, libp2p.EnableHolePunching(holepunch.WithTracer(&holePunchTracer{})))
		}
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	setupNetworkMonitor(h)

	kdht, err := discovery.NewDHT(h, dht.ModeServer) // Start DHT in server mode

	if err != nil {
		h.Close()
		return nil, nil, fmt.Errorf("failed to create DHT: %w", err)
	}

	return h, kdht, nil
}

type holePunchTracer struct{}

func (t *holePunchTracer) Trace(evt *holepunch.Event) {
	log.Printf("[DCUtR] Hole Punch Event: %s (Remote: %s)", evt.Type, evt.Remote)
}

func setupNetworkMonitor(h host.Host) {
	// Subscribe to reachability changes
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	if err == nil {
		go func() {
			for e := range sub.Out() {
				evt := e.(event.EvtLocalReachabilityChanged)
				log.Printf("[AutoNAT] Reachability changed to: %s", evt.Reachability.String())
			}
		}()
	}

	// Subscribe to connection lifecycle events
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			log.Printf("[Network] Connected to %s", c.RemotePeer())
			logActiveConnections(n, c.RemotePeer())
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			log.Printf("[Network] Disconnected from %s", c.RemotePeer())
			logActiveConnections(n, c.RemotePeer())
		},
	})
}

func logActiveConnections(n network.Network, p peer.ID) {
	conns := n.ConnsToPeer(p)
	if len(conns) == 0 {
		log.Printf("[Network] No active connections to %s", p)
		return
	}

	log.Printf("[Network] Active connections to %s:", p)
	for i, conn := range conns {
		connType := "Direct"
		if _, err := conn.RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			connType = "Relay"
		}
		log.Printf("  %d) %s [%s]", i+1, connType, conn.RemoteMultiaddr())
	}
}
