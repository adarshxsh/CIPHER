package transport

import (
	"context"
	"fmt"
	"log"
	"time"

	"cipher/internal/discovery"
	"cipher/internal/protocol"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
	"github.com/pbnjay/memory"
)

type nodeOptions struct {
	connMgr         connmgr.ConnManager
	lowWater        int
	highWater       int
	gracePeriod     time.Duration
	rcmgr           network.ResourceManager
	limiter         rcmgr.Limiter
	memoryLimit     int64
	peerStreamLimit int
	partialLimits   *rcmgr.PartialLimitConfig
	extraOpts       []libp2p.Option
}

// NodeOption defines a functional option for configuring NewNode.
type NodeOption func(*nodeOptions) error

// WithConnManager sets a custom libp2p ConnectionManager.
func WithConnManager(cm connmgr.ConnManager) NodeOption {
	return func(opts *nodeOptions) error {
		opts.connMgr = cm
		return nil
	}
}

// WithConnLimits configures low/high watermarks and grace period for the connection manager.
func WithConnLimits(low, high int, grace time.Duration) NodeOption {
	return func(opts *nodeOptions) error {
		opts.lowWater = low
		opts.highWater = high
		opts.gracePeriod = grace
		return nil
	}
}

// WithResourceManager sets a custom libp2p ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(opts *nodeOptions) error {
		opts.rcmgr = rm
		return nil
	}
}

// WithRcmgrLimiter sets a custom rcmgr.Limiter used when creating the ResourceManager.
func WithRcmgrLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(opts *nodeOptions) error {
		opts.limiter = limiter
		return nil
	}
}

// WithMemoryLimit overrides default memory limit used for resource manager scaling.
func WithMemoryLimit(memBytes int64) NodeOption {
	return func(opts *nodeOptions) error {
		opts.memoryLimit = memBytes
		return nil
	}
}

// WithPeerStreamLimit sets a per-peer stream limit override in resource manager protocol defaults.
func WithPeerStreamLimit(limit int) NodeOption {
	return func(opts *nodeOptions) error {
		opts.peerStreamLimit = limit
		return nil
	}
}

// WithPartialLimits applies partial limit overrides to the resource manager configuration.
func WithPartialLimits(cfg rcmgr.PartialLimitConfig) NodeOption {
	return func(opts *nodeOptions) error {
		opts.partialLimits = &cfg
		return nil
	}
}

// WithLibp2pOptions passes additional libp2p options directly.
func WithLibp2pOptions(libp2pOpts ...libp2p.Option) NodeOption {
	return func(opts *nodeOptions) error {
		opts.extraOpts = append(opts.extraOpts, libp2pOpts...)
		return nil
	}
}

func newNodeOptions() *nodeOptions {
	return &nodeOptions{
		lowWater:    50,
		highWater:   200,
		gracePeriod: 1 * time.Minute,
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	cfg := newNodeOptions()
	for _, opt := range nodeOpts {
		if err := opt(cfg); err != nil {
			return nil, nil, fmt.Errorf("invalid node option: %w", err)
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

	// 1. Connection Manager Initialization
	if cfg.connMgr == nil {
		cm, err := p2pconnmgr.NewConnManager(
			cfg.lowWater,
			cfg.highWater,
			p2pconnmgr.WithGracePeriod(cfg.gracePeriod),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
		}
		cfg.connMgr = cm
	}
	opts = append(opts, libp2p.ConnectionManager(cfg.connMgr))

	// 2. Resource Manager Initialization
	if cfg.rcmgr == nil {
		if cfg.limiter == nil {
			scaledCfg := rcmgr.DefaultLimits
			libp2p.SetDefaultServiceLimits(&scaledCfg)

			// Add CIPHER & standard IPFS protocol limits
			scaledCfg.AddProtocolLimit(protocol.ChunkTransportProtocolID, rcmgr.BaseLimit{Streams: 100, Memory: 64 * 1024 * 1024}, rcmgr.BaseLimitIncrease{})
			scaledCfg.AddProtocolLimit(protocol.FileTransferProtocolID, rcmgr.BaseLimit{Streams: 100, Memory: 64 * 1024 * 1024}, rcmgr.BaseLimitIncrease{})
			scaledCfg.AddProtocolLimit("/ipfs/id/1.0.0", rcmgr.BaseLimit{Streams: 100, Memory: 32 * 1024 * 1024}, rcmgr.BaseLimitIncrease{})
			scaledCfg.AddProtocolLimit("/ipfs/kad/1.0.0", rcmgr.BaseLimit{Streams: 100, Memory: 32 * 1024 * 1024}, rcmgr.BaseLimitIncrease{})

			if cfg.peerStreamLimit > 0 {
				scaledCfg.ProtocolPeerBaseLimit.Streams = cfg.peerStreamLimit
			}

			memLimit := cfg.memoryLimit
			if memLimit <= 0 {
				memLimit = int64(memory.TotalMemory())
			}
			if memLimit <= 0 {
				memLimit = 1024 * 1024 * 1024 // 1 GB fallback
			}

			fds := getFDLimit()
			scaledLimits := scaledCfg.Scale(memLimit, fds)

			if cfg.partialLimits != nil {
				scaledLimits = cfg.partialLimits.Build(scaledLimits)
			}

			cfg.limiter = rcmgr.NewFixedLimiter(scaledLimits)
		}

		rm, err := rcmgr.NewResourceManager(cfg.limiter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		cfg.rcmgr = rm
	}
	opts = append(opts, libp2p.ResourceManager(cfg.rcmgr))

	if len(cfg.extraOpts) > 0 {
		opts = append(opts, cfg.extraOpts...)
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
