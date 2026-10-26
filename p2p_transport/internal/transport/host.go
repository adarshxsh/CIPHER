package transport

import (
	"cipher/internal/discovery"
	"context"

	"fmt"
	"log"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

type nodeConfig struct {
	rm              network.ResourceManager
	resCfg          *ResourceConfig
	role            NodeRole
	connManager     connmgr.ConnManager
	connLimits      *connLimits
	memoryLimit     int64
	peerStreamLimit int
	partialLimits   *rcmgr.PartialLimitConfig
	rcmgrLimiter    rcmgr.Limiter
}

type connLimits struct {
	low  int
	high int
}

// NodeOption configures options for libp2p node creation.
type NodeOption func(*nodeConfig)

// WithResourceManager sets an explicit network.ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.rm = rm
	}
}

// WithResourceConfig configures the node with a ResourceConfig struct.
func WithResourceConfig(resCfg ResourceConfig) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.resCfg = &resCfg
	}
}

// WithRole specifies the NodeRole operational profile for resource limits.
func WithRole(role NodeRole) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.role = role
	}
}

// WithMemoryLimit sets a system memory limit override in bytes.
func WithMemoryLimit(limit int64) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.memoryLimit = limit
	}
}

// WithConnLimits sets connection watermarks.
func WithConnLimits(low, high int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.connLimits = &connLimits{low: low, high: high}
	}
}

// WithPeerStreamLimit sets maximum per-peer stream limits.
func WithPeerStreamLimit(limit int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.peerStreamLimit = limit
	}
}

// WithPartialLimits provides a PartialLimitConfig override.
func WithPartialLimits(p rcmgr.PartialLimitConfig) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.partialLimits = &p
	}
}

// WithRcmgrLimiter sets a custom rcmgr.Limiter.
func WithRcmgrLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.rcmgrLimiter = limiter
	}
}

// WithConnManager sets a custom connection manager.
func WithConnManager(cm connmgr.ConnManager) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.connManager = cm
	}
}

// NewHost creates a new libp2p host (alias for NewNode).
func NewHost(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	return NewNode(ctx, listenPort, wsPort, priv, relayAddr, forceRelay, nodeOpts...)
}

// NewNode creates a new libp2p host with configurable resource management.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	nc := &nodeConfig{}
	for _, opt := range nodeOpts {
		opt(nc)
	}

	var rm network.ResourceManager
	var err error

	if nc.rm != nil {
		rm = nc.rm
	} else if nc.rcmgrLimiter != nil {
		rm, err = rcmgr.NewResourceManager(nc.rcmgrLimiter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager from limiter: %w", err)
		}
	} else {
		var resCfg ResourceConfig
		if nc.resCfg != nil {
			resCfg = *nc.resCfg
		} else {
			resCfg, err = LoadResourceConfigFromEnv()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to load resource config from environment: %w", err)
			}
		}

		if nc.role != "" {
			resCfg.Role = nc.role
		}
		if nc.memoryLimit > 0 {
			resCfg.MaxMemory = nc.memoryLimit
		}
		if nc.peerStreamLimit > 0 {
			resCfg.PeerStreamLimit = nc.peerStreamLimit
		}
		if nc.connLimits != nil {
			resCfg.MaxConns = nc.connLimits.high
			resCfg.InboundConns = nc.connLimits.high
			resCfg.OutboundConns = nc.connLimits.low
		}

		rm, err = resCfg.BuildResourceManager()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build resource manager: %w", err)
		}
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
		libp2p.ResourceManager(rm),
	}

	if nc.connManager != nil {
		opts = append(opts, libp2p.ConnectionManager(nc.connManager))
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
