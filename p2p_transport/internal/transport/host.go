package transport

import (
	"context"
	"fmt"
	"log"

	"cipher/internal/discovery"
	"cipher/internal/protocol"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// NodeConfig holds configuration parameters for host creation and resource management.
type NodeConfig struct {
	Allowlist       []multiaddr.Multiaddr
	Limits          *rcmgr.ScalingLimitConfig
	ResourceManager network.ResourceManager
	MaxMemory       int64
}

// NodeOption configures options for libp2p host construction.
type NodeOption func(*NodeConfig)

// WithAllowlist adds multiaddrs to the resource manager allowlist.
func WithAllowlist(mas []multiaddr.Multiaddr) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.Allowlist = append(cfg.Allowlist, mas...)
	}
}

// WithScalingLimits overrides default scaling limit configuration.
func WithScalingLimits(limits rcmgr.ScalingLimitConfig) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.Limits = &limits
	}
}

// WithResourceManager directly supplies a pre-configured network.ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.ResourceManager = rm
	}
}

// WithMaxMemory sets an upper limit on heap memory allocated for libp2p buffers.
func WithMaxMemory(maxMem int64) NodeOption {
	return func(cfg *NodeConfig) {
		cfg.MaxMemory = maxMem
	}
}

// DefaultScalingLimits constructs a multi-tiered ScalingLimitConfig tailored for CIPHER.
func DefaultScalingLimits() rcmgr.ScalingLimitConfig {
	limits := rcmgr.DefaultLimits

	// Register default service limits for bundled libp2p background services
	libp2p.SetDefaultServiceLimits(&limits)

	// Enforce per-peer connection caps (maximum 8 inbound and 8 outbound per peer ID)
	limits.PeerBaseLimit.ConnsInbound = 8
	limits.PeerBaseLimit.ConnsOutbound = 8
	limits.PeerBaseLimit.Conns = 8
	limits.PeerLimitIncrease.ConnsInbound = 0
	limits.PeerLimitIncrease.ConnsOutbound = 0
	limits.PeerLimitIncrease.Conns = 0

	// CIPHER Chunk Transport protocol (/cipher/chunk/1.0.0)
	limits.AddProtocolLimit(
		protocol.ChunkTransportProtocolID,
		rcmgr.BaseLimit{
			StreamsInbound:  256,
			StreamsOutbound: 512,
			Streams:         512,
			Memory:          128 << 20, // 128 MB
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  128,
			StreamsOutbound: 256,
			Streams:         256,
			Memory:          64 << 20, // 64 MB
		},
	)
	limits.AddProtocolPeerLimit(
		protocol.ChunkTransportProtocolID,
		rcmgr.BaseLimit{
			StreamsInbound:  16,
			StreamsOutbound: 32,
			Streams:         32,
			Memory:          16 << 20, // 16 MB
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  4,
			StreamsOutbound: 8,
			Streams:         8,
			Memory:          4 << 20, // 4 MB
		},
	)

	// Legacy File Transfer protocol (/cipher/filetransfer/1.0.0)
	limits.AddProtocolLimit(
		protocol.FileTransferProtocolID,
		rcmgr.BaseLimit{
			StreamsInbound:  128,
			StreamsOutbound: 256,
			Streams:         256,
			Memory:          64 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  64,
			StreamsOutbound: 128,
			Streams:         128,
			Memory:          32 << 20,
		},
	)
	limits.AddProtocolPeerLimit(
		protocol.FileTransferProtocolID,
		rcmgr.BaseLimit{
			StreamsInbound:  8,
			StreamsOutbound: 16,
			Streams:         16,
			Memory:          8 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  2,
			StreamsOutbound: 4,
			Streams:         4,
			Memory:          2 << 20,
		},
	)

	// Discovery Protocols: Identify (/ipfs/id/1.0.0) & Kad-DHT (/ipfs/kad/1.0.0)
	limits.AddProtocolLimit(
		"/ipfs/id/1.0.0",
		rcmgr.BaseLimit{
			StreamsInbound:  128,
			StreamsOutbound: 128,
			Streams:         256,
			Memory:          16 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  32,
			StreamsOutbound: 32,
			Streams:         64,
			Memory:          8 << 20,
		},
	)
	limits.AddProtocolPeerLimit(
		"/ipfs/id/1.0.0",
		rcmgr.BaseLimit{
			StreamsInbound:  4,
			StreamsOutbound: 4,
			Streams:         8,
			Memory:          4 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  1,
			StreamsOutbound: 1,
			Streams:         2,
			Memory:          1 << 20,
		},
	)

	limits.AddProtocolLimit(
		"/ipfs/kad/1.0.0",
		rcmgr.BaseLimit{
			StreamsInbound:  256,
			StreamsOutbound: 512,
			Streams:         512,
			Memory:          64 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  64,
			StreamsOutbound: 128,
			Streams:         128,
			Memory:          32 << 20,
		},
	)
	limits.AddProtocolPeerLimit(
		"/ipfs/kad/1.0.0",
		rcmgr.BaseLimit{
			StreamsInbound:  8,
			StreamsOutbound: 16,
			Streams:         16,
			Memory:          8 << 20,
		},
		rcmgr.BaseLimitIncrease{
			StreamsInbound:  2,
			StreamsOutbound: 4,
			Streams:         4,
			Memory:          2 << 20,
		},
	)

	return limits
}

// NewDefaultResourceManager builds a network.ResourceManager with scaling limits and optional allowlist parameters.
func NewDefaultResourceManager(opts ...NodeOption) (network.ResourceManager, error) {
	var config NodeConfig
	for _, opt := range opts {
		opt(&config)
	}

	if config.ResourceManager != nil {
		return config.ResourceManager, nil
	}

	var limits rcmgr.ScalingLimitConfig
	if config.Limits != nil {
		limits = *config.Limits
	} else {
		limits = DefaultScalingLimits()
	}

	var scaled rcmgr.ConcreteLimitConfig
	if config.MaxMemory > 0 {
		scaled = limits.Scale(config.MaxMemory, 0)
	} else {
		scaled = limits.AutoScale()
	}

	limiter := rcmgr.NewFixedLimiter(scaled)

	var rcmgrOpts []rcmgr.Option
	if len(config.Allowlist) > 0 {
		rcmgrOpts = append(rcmgrOpts, rcmgr.WithAllowlistedMultiaddrs(config.Allowlist))
	}

	rm, err := rcmgr.NewResourceManager(limiter, rcmgrOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}

	return rm, nil
}

// NewNode creates a new libp2p host.
func NewNode(
	ctx context.Context,
	listenPort int,
	wsPort int,
	priv crypto.PrivKey,
	relayAddr string,
	forceRelay bool,
	options ...NodeOption,
) (host.Host, *dht.IpfsDHT, error) {
	var nodeConfig NodeConfig
	for _, opt := range options {
		opt(&nodeConfig)
	}

	addr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	listenAddrs := []string{addr}
	if wsPort > 0 {
		wsAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", wsPort)
		listenAddrs = append(listenAddrs, wsAddr)
	}

	rm := nodeConfig.ResourceManager
	if rm == nil {
		var err error
		rm, err = NewDefaultResourceManager(options...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build resource manager: %w", err)
		}
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
