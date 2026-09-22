package transport

import (
	"cipher/internal/discovery"
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
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// rejectionLogger satisfies rcmgr.TraceReporter to log resource manager rejection events.
type rejectionLogger struct{}

func (l *rejectionLogger) ConsumeEvent(evt rcmgr.TraceEvt) {
	switch evt.Type {
	case rcmgr.TraceBlockAddStreamEvt, rcmgr.TraceBlockAddConnEvt, rcmgr.TraceBlockReserveMemoryEvt:
		log.Printf("[ResourceManager] Rejection event: %s (name: %s, delta: %d, memory: %d, streamsIn: %d, streamsOut: %d, connsIn: %d, connsOut: %d)",
			evt.Type, evt.Name, evt.Delta, evt.Memory, evt.StreamsIn, evt.StreamsOut, evt.ConnsIn, evt.ConnsOut)
	}
}

// NewNode creates a new libp2p host with resource bounds and connection manager options.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	cfg := DefaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	listenAddrs := []string{addr}
	if wsPort > 0 {
		wsAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsPort)
		listenAddrs = append(listenAddrs, wsAddr)
	}

	libp2pOpts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
	}

	if priv != nil {
		libp2pOpts = append(libp2pOpts, libp2p.Identity(priv))
	}

	// 1. Connection Manager
	cm, err := connmgr.NewConnManager(cfg.ConnLowWatermark, cfg.ConnHighWatermark, connmgr.WithGracePeriod(cfg.ConnGracePeriod))
	if err == nil {
		libp2pOpts = append(libp2pOpts, libp2p.ConnectionManager(cm))
	}

	// 2. Resource Manager
	rm := cfg.ResourceManager
	if rm == nil {
		limiter := cfg.Limiter
		if limiter == nil {
			scaledLimits := rcmgr.DefaultLimits
			libp2p.SetDefaultServiceLimits(&scaledLimits)

			// Add protocol limits for CIPHER transfer protocols
			scaledLimits.AddProtocolLimit(protocol.ID("/cipher/chunk/1.0.0"), rcmgr.BaseLimit{Streams: 1024, StreamsInbound: 512, StreamsOutbound: 512, Memory: 64 << 20}, rcmgr.BaseLimitIncrease{})
			scaledLimits.AddProtocolLimit(protocol.ID("/cipher/filetransfer/1.0.0"), rcmgr.BaseLimit{Streams: 1024, StreamsInbound: 512, StreamsOutbound: 512, Memory: 64 << 20}, rcmgr.BaseLimitIncrease{})

			scaledLimits.AddProtocolPeerLimit(protocol.ID("/cipher/chunk/1.0.0"), rcmgr.BaseLimit{Streams: cfg.MaxPeerStreams, StreamsInbound: cfg.MaxPeerStreams, StreamsOutbound: cfg.MaxPeerStreams, Memory: 16 << 20}, rcmgr.BaseLimitIncrease{})
			scaledLimits.AddProtocolPeerLimit(protocol.ID("/cipher/filetransfer/1.0.0"), rcmgr.BaseLimit{Streams: cfg.MaxPeerStreams, StreamsInbound: cfg.MaxPeerStreams, StreamsOutbound: cfg.MaxPeerStreams, Memory: 16 << 20}, rcmgr.BaseLimitIncrease{})

			// System total connection cap (e.g., 100)
			if cfg.MaxTotalConns > 0 {
				scaledLimits.SystemBaseLimit.Conns = cfg.MaxTotalConns
				scaledLimits.SystemBaseLimit.ConnsInbound = cfg.MaxTotalConns
				scaledLimits.SystemBaseLimit.ConnsOutbound = cfg.MaxTotalConns
				scaledLimits.SystemLimitIncrease.Conns = 0
				scaledLimits.SystemLimitIncrease.ConnsInbound = 0
				scaledLimits.SystemLimitIncrease.ConnsOutbound = 0
			}

			// Per-peer stream cap (e.g., 16)
			if cfg.MaxPeerStreams > 0 {
				scaledLimits.PeerBaseLimit.Streams = cfg.MaxPeerStreams
				scaledLimits.PeerBaseLimit.StreamsInbound = cfg.MaxPeerStreams
				scaledLimits.PeerBaseLimit.StreamsOutbound = cfg.MaxPeerStreams
				scaledLimits.PeerLimitIncrease.Streams = 0
				scaledLimits.PeerLimitIncrease.StreamsInbound = 0
				scaledLimits.PeerLimitIncrease.StreamsOutbound = 0
			}

			var concrete rcmgr.ConcreteLimitConfig
			if cfg.MaxMemoryBytes > 0 {
				concrete = scaledLimits.Scale(cfg.MaxMemoryBytes, 1024)
			} else {
				concrete = scaledLimits.AutoScale()
			}

			if cfg.PartialLimits != nil {
				concrete = cfg.PartialLimits.Build(concrete)
			}

			limiter = rcmgr.NewFixedLimiter(concrete)
		}

		rcmgrOptions := []rcmgr.Option{
			rcmgr.WithTraceReporter(&rejectionLogger{}),
		}
		rcmgrOptions = append(rcmgrOptions, cfg.RcmgrOpts...)

		var rmErr error
		rm, rmErr = rcmgr.NewResourceManager(limiter, rcmgrOptions...)
		if rmErr != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", rmErr)
		}
	}

	libp2pOpts = append(libp2pOpts, libp2p.ResourceManager(rm))

	if relayAddr != "" {
		maddr, err := multiaddr.NewMultiaddr(relayAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid relay address: %w", err)
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid relay peer info: %w", err)
		}
		libp2pOpts = append(libp2pOpts,
			libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*addrInfo}),
			libp2p.ForceReachabilityPrivate(), // Force Private to instantly enable Hole Punching!
		)
		if !forceRelay {
			libp2pOpts = append(libp2pOpts, libp2p.EnableHolePunching(holepunch.WithTracer(&holePunchTracer{})))
		}
	}

	h, err := libp2p.New(libp2pOpts...)
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

// NewHost creates a new libp2p host (alias for NewNode).
func NewHost(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	return NewNode(ctx, listenPort, wsPort, priv, relayAddr, forceRelay, opts...)
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
