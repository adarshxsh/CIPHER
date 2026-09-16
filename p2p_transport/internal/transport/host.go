package transport

import (
	"cipher/internal/discovery"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	p2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// NewNode creates a new libp2p host with resource limits and connection limits.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	return NewHost(ctx, listenPort, wsPort, priv, relayAddr, forceRelay, opts...)
}

// NewHost constructs a libp2p host with rcmgr.ResourceManager enforcing swarm connection and stream limits.
func NewHost(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	var nOpts nodeOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&nOpts)
		}
	}

	if nOpts.connManager == nil {
		lowWatermark := 50
		highWatermark := 200
		gracePeriod := 1 * time.Minute

		if nOpts.lowWatermark > 0 {
			lowWatermark = nOpts.lowWatermark
		}
		if nOpts.highWatermark > 0 {
			highWatermark = nOpts.highWatermark
		}
		if nOpts.gracePeriod > 0 {
			gracePeriod = nOpts.gracePeriod
		}

		cm, err := p2pconnmgr.NewConnManager(lowWatermark, highWatermark, p2pconnmgr.WithGracePeriod(gracePeriod))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
		}
		nOpts.connManager = cm
	}

	if nOpts.resourceManager == nil {
		maxFDs := getMaxFDs()

		memCap := int64(256 * 1024 * 1024) // 256 MiB default
		if nOpts.maxMemory > 0 {
			memCap = nOpts.maxMemory
		}

		scaledDefaults := rcmgr.DefaultLimits.Scale(memCap, maxFDs)

		defaultPartial := rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				Conns:         rcmgr.LimitVal(1024),
				ConnsInbound:  rcmgr.LimitVal(1024),
				ConnsOutbound: rcmgr.LimitVal(1024),
				Memory:        rcmgr.LimitVal64(memCap),
				FD:            rcmgr.LimitVal(maxFDs),
			},
			PeerDefault: rcmgr.ResourceLimits{
				Streams:         rcmgr.LimitVal(64),
				StreamsInbound:  rcmgr.LimitVal(64),
				StreamsOutbound: rcmgr.LimitVal(64),
			},
			ProtocolPeerDefault: rcmgr.ResourceLimits{
				Streams:         rcmgr.LimitVal(64),
				StreamsInbound:  rcmgr.LimitVal(64),
				StreamsOutbound: rcmgr.LimitVal(64),
			},
		}

		if nOpts.partialLimits != nil {
			applyPartialLimitOverrides(&defaultPartial, *nOpts.partialLimits)
		}

		concreteLimits := defaultPartial.Build(scaledDefaults)
		limiter := rcmgr.NewFixedLimiter(concreteLimits)

		var rcmgrOpts []rcmgr.Option
		if nOpts.metricsReporter != nil {
			rcmgrOpts = append(rcmgrOpts, rcmgr.WithMetrics(nOpts.metricsReporter))
		}

		rm, err := rcmgr.NewResourceManager(limiter, rcmgrOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		nOpts.resourceManager = rm
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
		libp2p.ConnectionManager(nOpts.connManager),
		libp2p.ResourceManager(nOpts.resourceManager),
	}

	if priv != nil {
		libp2pOpts = append(libp2pOpts, libp2p.Identity(priv))
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
