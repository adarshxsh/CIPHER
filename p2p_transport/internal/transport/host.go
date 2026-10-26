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
	coreconnmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// NodeConfig holds configuration settings for transport host initialization.
type NodeConfig struct {
	LowWatermark    int
	HighWatermark   int
	GracePeriod     time.Duration
	MemoryLimitMB   int
	ConnManager     coreconnmgr.ConnManager
	ResourceManager network.ResourceManager
	Limiter         rcmgr.Limiter
}

// NodeOption defines a functional option for configuring host parameters.
type NodeOption func(*NodeConfig)

// Option is an alias for NodeOption.
type Option = NodeOption

// WithConnLimits sets the low and high watermarks and grace period for ConnectionManager.
func WithConnLimits(low, high int, gracePeriod time.Duration) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.LowWatermark = low
		cfg.HighWatermark = high
		cfg.GracePeriod = gracePeriod
	}
}

// WithMinConns sets the low watermark for ConnectionManager.
func WithMinConns(minConns int) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.LowWatermark = minConns
	}
}

// WithMaxConns sets the high watermark for ConnectionManager.
func WithMaxConns(maxConns int) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.HighWatermark = maxConns
	}
}

// WithGracePeriod sets the grace period for ConnectionManager.
func WithGracePeriod(gracePeriod time.Duration) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.GracePeriod = gracePeriod
	}
}

// WithMemoryLimitMB sets explicit memory cap in MB for ResourceManager.
func WithMemoryLimitMB(memMB int) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.MemoryLimitMB = memMB
	}
}

// WithConnManager specifies a custom libp2p ConnectionManager.
func WithConnManager(cm coreconnmgr.ConnManager) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.ConnManager = cm
	}
}

// WithResourceManager specifies a custom libp2p ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.ResourceManager = rm
	}
}

// WithLimiter specifies a custom libp2p rcmgr.Limiter for ResourceManager.
func WithLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.Limiter = limiter
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	nodeCfg := &NodeConfig{
		LowWatermark:  100,
		HighWatermark: 400,
		GracePeriod:   1 * time.Minute,
		MemoryLimitMB: 0,
	}

	for _, opt := range nodeOpts {
		if opt != nil {
			opt(nodeCfg)
		}
	}

	if nodeCfg.LowWatermark <= 0 {
		nodeCfg.LowWatermark = 100
	}
	if nodeCfg.HighWatermark < nodeCfg.LowWatermark {
		nodeCfg.HighWatermark = nodeCfg.LowWatermark + 100
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
	}

	if priv != nil {
		opts = append(opts, libp2p.Identity(priv))
	}

	if nodeCfg.ConnManager != nil {
		opts = append(opts, libp2p.ConnectionManager(nodeCfg.ConnManager))
	} else {
		cm, err := connmgr.NewConnManager(
			nodeCfg.LowWatermark,
			nodeCfg.HighWatermark,
			connmgr.WithGracePeriod(nodeCfg.GracePeriod),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
		}
		opts = append(opts, libp2p.ConnectionManager(cm))
	}

	if nodeCfg.ResourceManager != nil {
		opts = append(opts, libp2p.ResourceManager(nodeCfg.ResourceManager))
	} else {
		var rm network.ResourceManager
		var err error
		if nodeCfg.Limiter != nil {
			rm, err = rcmgr.NewResourceManager(nodeCfg.Limiter)
		} else {
			scalingLimits := rcmgr.DefaultLimits
			libp2p.SetDefaultServiceLimits(&scalingLimits)
			var concreteLimits rcmgr.ConcreteLimitConfig
			if nodeCfg.MemoryLimitMB > 0 {
				bytesLimit := int64(nodeCfg.MemoryLimitMB) * 1024 * 1024
				concreteLimits = scalingLimits.Scale(bytesLimit, 0)
			} else {
				concreteLimits = scalingLimits.AutoScale()
			}
			limiter := rcmgr.NewFixedLimiter(concreteLimits)
			rm, err = rcmgr.NewResourceManager(limiter)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		opts = append(opts, libp2p.ResourceManager(rm))
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
