package transport

import (
	"cipher/internal/discovery"
	"context"

	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
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

type nodeOptions struct {
	connMgr         coreconnmgr.ConnManager
	connLowWater    int
	connHighWater   int
	connGracePeriod time.Duration

	resourceMgr    network.ResourceManager
	rcmgrLimiter   rcmgr.Limiter
	maxMemoryBytes int64
	maxPeerStreams int
	partialLimits  *rcmgr.PartialLimitConfig
}

// NodeOption defines a functional option for configuring a node.
type NodeOption func(*nodeOptions)

// WithConnManager supplies a custom libp2p ConnManager.
func WithConnManager(cm coreconnmgr.ConnManager) NodeOption {
	return func(o *nodeOptions) {
		o.connMgr = cm
	}
}

// WithConnLimits configures custom low and high connection watermarks and grace period.
func WithConnLimits(low, high int, gracePeriod time.Duration) NodeOption {
	return func(o *nodeOptions) {
		o.connLowWater = low
		o.connHighWater = high
		o.connGracePeriod = gracePeriod
	}
}

// WithResourceManager supplies a custom libp2p ResourceManager.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(o *nodeOptions) {
		o.resourceMgr = rm
	}
}

// WithRcmgrLimiter supplies a custom rcmgr.Limiter.
func WithRcmgrLimiter(limiter rcmgr.Limiter) NodeOption {
	return func(o *nodeOptions) {
		o.rcmgrLimiter = limiter
	}
}

// WithMemoryLimit sets the maximum libp2p system memory limit in bytes (default: 256 MiB).
func WithMemoryLimit(bytes int64) NodeOption {
	return func(o *nodeOptions) {
		o.maxMemoryBytes = bytes
	}
}

// WithPeerStreamLimit sets the maximum number of concurrent streams per peer (default: 64).
func WithPeerStreamLimit(limit int) NodeOption {
	return func(o *nodeOptions) {
		o.maxPeerStreams = limit
	}
}

// WithPartialLimits supplies a custom rcmgr.PartialLimitConfig to override specific limits.
func WithPartialLimits(limits rcmgr.PartialLimitConfig) NodeOption {
	return func(o *nodeOptions) {
		o.partialLimits = &limits
	}
}

func defaultNodeOptions() *nodeOptions {
	return &nodeOptions{
		connLowWater:    50,
		connHighWater:   200,
		connGracePeriod: time.Minute,
		maxMemoryBytes:  256 * 1024 * 1024, // 256 MiB
		maxPeerStreams:  64,
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	nOpts := defaultNodeOptions()
	for _, opt := range nodeOpts {
		opt(nOpts)
	}

	if nOpts.connMgr == nil {
		cm, err := connmgr.NewConnManager(
			nOpts.connLowWater,
			nOpts.connHighWater,
			connmgr.WithGracePeriod(nOpts.connGracePeriod),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create conn manager: %w", err)
		}
		nOpts.connMgr = cm
	}

	if nOpts.resourceMgr == nil {
		if nOpts.rcmgrLimiter == nil {
			scaledDefault := rcmgr.DefaultLimits
			libp2p.SetDefaultServiceLimits(&scaledDefault)

			var partial rcmgr.PartialLimitConfig
			if nOpts.partialLimits != nil {
				partial = *nOpts.partialLimits
			}

			if nOpts.maxMemoryBytes > 0 && partial.System.Memory == 0 {
				partial.System.Memory = rcmgr.LimitVal64(nOpts.maxMemoryBytes)
			}

			if nOpts.maxPeerStreams > 0 {
				if partial.PeerDefault.Streams == 0 {
					partial.PeerDefault.Streams = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}
				if partial.PeerDefault.StreamsInbound == 0 {
					partial.PeerDefault.StreamsInbound = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}
				if partial.PeerDefault.StreamsOutbound == 0 {
					partial.PeerDefault.StreamsOutbound = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}

				if partial.ProtocolPeerDefault.Streams == 0 {
					partial.ProtocolPeerDefault.Streams = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}
				if partial.ProtocolPeerDefault.StreamsInbound == 0 {
					partial.ProtocolPeerDefault.StreamsInbound = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}
				if partial.ProtocolPeerDefault.StreamsOutbound == 0 {
					partial.ProtocolPeerDefault.StreamsOutbound = rcmgr.LimitVal(nOpts.maxPeerStreams)
				}
			}

			concrete := partial.Build(scaledDefault.AutoScale())
			nOpts.rcmgrLimiter = rcmgr.NewFixedLimiter(concrete)
		}

		rm, err := rcmgr.NewResourceManager(nOpts.rcmgrLimiter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		nOpts.resourceMgr = rm
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
		libp2p.ConnectionManager(nOpts.connMgr),
		libp2p.ResourceManager(nOpts.resourceMgr),
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
