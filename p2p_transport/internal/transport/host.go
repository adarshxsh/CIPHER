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
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

type nodeConfig struct {
	connManager     coreconnmgr.ConnManager
	resourceManager network.ResourceManager

	lowWatermark  int
	highWatermark int
	gracePeriod   time.Duration

	memoryLimit     int64
	peerStreamLimit int
	rcmgrLimiter    rcmgr.Limiter
	partialLimits   *rcmgr.PartialLimitConfig
}

// NodeOption defines functional options for configuring NewNode host parameters.
type NodeOption func(*nodeConfig)

// WithConnManager sets a custom ConnManager.
func WithConnManager(cm coreconnmgr.ConnManager) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.connManager = cm
	}
}

// WithResourceManager sets a custom ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.resourceManager = rm
	}
}

// WithConnLimits sets custom low and high connection watermarks and grace period.
func WithConnLimits(low, high int, grace time.Duration) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.lowWatermark = low
		cfg.highWatermark = high
		cfg.gracePeriod = grace
	}
}

// WithMemoryLimit sets a custom system memory limit in bytes.
func WithMemoryLimit(memBytes int64) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.memoryLimit = memBytes
	}
}

// WithPeerStreamLimit sets a custom per-peer stream limit.
func WithPeerStreamLimit(limit int) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.peerStreamLimit = limit
	}
}

// WithPartialLimits applies partial limit overrides to the default concrete scaling limits.
func WithPartialLimits(partial rcmgr.PartialLimitConfig) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.partialLimits = &partial
	}
}

// WithRcmgrLimiter sets a custom rcmgr.Limiter.
func WithRcmgrLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.rcmgrLimiter = limiter
	}
}

// NewNode creates a new libp2p host with ConnectionManager and ResourceManager options.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	cfg := &nodeConfig{
		lowWatermark:  50,
		highWatermark: 200,
		gracePeriod:   1 * time.Minute,
	}

	for _, opt := range nodeOpts {
		if opt != nil {
			opt(cfg)
		}
	}

	// 1. Configure ConnectionManager
	cm := cfg.connManager
	if cm == nil {
		var err error
		cm, err = connmgr.NewConnManager(
			cfg.lowWatermark,
			cfg.highWatermark,
			connmgr.WithGracePeriod(cfg.gracePeriod),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
		}
	}

	// 2. Configure ResourceManager
	rm := cfg.resourceManager
	if rm == nil {
		if cfg.rcmgrLimiter != nil {
			var err error
			rm, err = rcmgr.NewResourceManager(cfg.rcmgrLimiter)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create resource manager from limiter: %w", err)
			}
		} else {
			scalingLimits := rcmgr.DefaultLimits
			libp2p.SetDefaultServiceLimits(&scalingLimits)

			// Enforce per-peer connection caps (maximum 8 inbound/outbound, 16 total)
			scalingLimits.PeerBaseLimit.ConnsInbound = 8
			scalingLimits.PeerBaseLimit.ConnsOutbound = 8
			scalingLimits.PeerBaseLimit.Conns = 16

			// Enforce protocol limits for cipher protocols & standard IPFS protocols
			chunkProto := protocol.ID("/cipher/chunk/1.0.0")
			ftProto := protocol.ID("/cipher/filetransfer/1.0.0")
			idProto := protocol.ID("/ipfs/id/1.0.0")
			kadProto := protocol.ID("/ipfs/kad/1.0.0")

			baseStreamLimit := 1024
			if cfg.peerStreamLimit > 0 {
				baseStreamLimit = cfg.peerStreamLimit
			}

			scalingLimits.AddProtocolLimit(chunkProto, rcmgr.BaseLimit{Streams: baseStreamLimit, StreamsInbound: baseStreamLimit / 2, StreamsOutbound: baseStreamLimit / 2}, rcmgr.BaseLimitIncrease{})
			scalingLimits.AddProtocolLimit(ftProto, rcmgr.BaseLimit{Streams: baseStreamLimit, StreamsInbound: baseStreamLimit / 2, StreamsOutbound: baseStreamLimit / 2}, rcmgr.BaseLimitIncrease{})
			scalingLimits.AddProtocolLimit(idProto, rcmgr.BaseLimit{Streams: baseStreamLimit, StreamsInbound: baseStreamLimit / 2, StreamsOutbound: baseStreamLimit / 2}, rcmgr.BaseLimitIncrease{})
			scalingLimits.AddProtocolLimit(kadProto, rcmgr.BaseLimit{Streams: baseStreamLimit, StreamsInbound: baseStreamLimit / 2, StreamsOutbound: baseStreamLimit / 2}, rcmgr.BaseLimitIncrease{})

			if cfg.peerStreamLimit > 0 {
				scalingLimits.AddProtocolPeerLimit(chunkProto, rcmgr.BaseLimit{Streams: cfg.peerStreamLimit, StreamsInbound: cfg.peerStreamLimit, StreamsOutbound: cfg.peerStreamLimit}, rcmgr.BaseLimitIncrease{})
				scalingLimits.AddProtocolPeerLimit(ftProto, rcmgr.BaseLimit{Streams: cfg.peerStreamLimit, StreamsInbound: cfg.peerStreamLimit, StreamsOutbound: cfg.peerStreamLimit}, rcmgr.BaseLimitIncrease{})
			}

			if cfg.memoryLimit > 0 {
				scalingLimits.SystemBaseLimit.Memory = cfg.memoryLimit
			}

			concrete := scalingLimits.AutoScale()
			if cfg.partialLimits != nil {
				concrete = cfg.partialLimits.Build(concrete)
			}

			limiter := rcmgr.NewFixedLimiter(concrete)
			var err error
			rm, err = rcmgr.NewResourceManager(limiter)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
			}
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

// NewHost creates a new libp2p host (alias for NewNode).
func NewHost(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	return NewNode(ctx, listenPort, wsPort, priv, relayAddr, forceRelay, nodeOpts...)
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
