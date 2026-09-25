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
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// NewNode creates a new libp2p host with role-based quota configuration.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	nOpts := nodeOptions{
		quotaConfig: DefaultQuotaConfig(),
	}
	for _, opt := range nodeOpts {
		if opt != nil {
			opt(&nOpts)
		}
	}

	cfg := nOpts.quotaConfig

	cm, err := connmgr.NewConnManager(
		cfg.ConnLow,
		cfg.ConnHigh,
		connmgr.WithGracePeriod(cfg.GracePeriod),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	scalingLimits := rcmgr.DefaultLimits
	if cfg.MaxMemory > 0 {
		scalingLimits.SystemBaseLimit.Memory = cfg.MaxMemory
	}
	if cfg.MaxStreams > 0 {
		scalingLimits.SystemBaseLimit.Streams = cfg.MaxStreams
	}
	if cfg.MaxStreamsInbound > 0 {
		scalingLimits.SystemBaseLimit.StreamsInbound = cfg.MaxStreamsInbound
	}
	if cfg.MaxStreamsOutbound > 0 {
		scalingLimits.SystemBaseLimit.StreamsOutbound = cfg.MaxStreamsOutbound
	}
	if cfg.MaxConns > 0 {
		scalingLimits.SystemBaseLimit.Conns = cfg.MaxConns
	}
	if cfg.MaxConnsInbound > 0 {
		scalingLimits.SystemBaseLimit.ConnsInbound = cfg.MaxConnsInbound
	}
	if cfg.MaxConnsOutbound > 0 {
		scalingLimits.SystemBaseLimit.ConnsOutbound = cfg.MaxConnsOutbound
	}

	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(scalingLimits.AutoScale()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	listenAddrs := []string{addr}
	if wsPort > 0 {
		wsAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsPort)
		listenAddrs = append(listenAddrs, wsAddr)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableRelay(),
		libp2p.ConnectionManager(cm),
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
