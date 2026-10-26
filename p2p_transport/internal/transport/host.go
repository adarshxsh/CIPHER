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
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

const (
	DefaultLowWatermark      = 50
	DefaultHighWatermark     = 100
	DefaultConnGracePeriod   = 1 * time.Minute
	DefaultSystemMemoryLimit = 256 * 1024 * 1024 // 256MB
	DefaultPeerMemoryLimit   = 16 * 1024 * 1024  // 16MB
	DefaultPeerStreamLimit   = 64
)

type nodeConfig struct {
	lowWatermark      int
	highWatermark     int
	connGracePeriod   time.Duration
	systemMemoryLimit int64
	peerMemoryLimit   int64
	peerStreamLimit   int
}

// NodeOption defines a functional option for configuring a libp2p node.
type NodeOption func(*nodeConfig)

func defaultConfig() *nodeConfig {
	return &nodeConfig{
		lowWatermark:      DefaultLowWatermark,
		highWatermark:     DefaultHighWatermark,
		connGracePeriod:   DefaultConnGracePeriod,
		systemMemoryLimit: DefaultSystemMemoryLimit,
		peerMemoryLimit:   DefaultPeerMemoryLimit,
		peerStreamLimit:   DefaultPeerStreamLimit,
	}
}

// WithConnLimits sets custom low and high connection watermarks.
func WithConnLimits(low, high int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.lowWatermark = low
		cfg.highWatermark = high
	}
}

// WithConnWatermarks is an alias for WithConnLimits.
func WithConnWatermarks(low, high int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.lowWatermark = low
		cfg.highWatermark = high
	}
}

// WithConnGracePeriod sets a custom connection manager grace period.
func WithConnGracePeriod(d time.Duration) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.connGracePeriod = d
	}
}

// WithMemoryLimits sets custom system and peer memory limits in bytes.
func WithMemoryLimits(systemMem, peerMem int64) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.systemMemoryLimit = systemMem
		cfg.peerMemoryLimit = peerMem
	}
}

// WithSystemMemoryLimit sets custom system memory limit in bytes.
func WithSystemMemoryLimit(bytes int64) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.systemMemoryLimit = bytes
	}
}

// WithPeerMemoryLimit sets custom peer memory limit in bytes.
func WithPeerMemoryLimit(bytes int64) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.peerMemoryLimit = bytes
	}
}

// WithPeerStreamLimit sets custom per-peer stream limit.
func WithPeerStreamLimit(limit int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.peerStreamLimit = limit
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	nodeCfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(nodeCfg)
		}
	}

	cm, err := p2pconnmgr.NewConnManager(
		nodeCfg.lowWatermark,
		nodeCfg.highWatermark,
		p2pconnmgr.WithGracePeriod(nodeCfg.connGracePeriod),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	pLimits := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Memory: rcmgr.LimitVal64(nodeCfg.systemMemoryLimit),
		},
		PeerDefault: rcmgr.ResourceLimits{
			Memory:          rcmgr.LimitVal64(nodeCfg.peerMemoryLimit),
			Streams:         rcmgr.LimitVal(nodeCfg.peerStreamLimit),
			StreamsInbound:  rcmgr.LimitVal(nodeCfg.peerStreamLimit),
			StreamsOutbound: rcmgr.LimitVal(nodeCfg.peerStreamLimit),
		},
	}
	concreteLimits := pLimits.Build(rcmgr.DefaultLimits.AutoScale())
	limiter := rcmgr.NewFixedLimiter(concreteLimits)
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
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
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rm),
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
