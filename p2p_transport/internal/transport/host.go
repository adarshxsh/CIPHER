package transport

import (
	"cipher/internal/discovery"
	"cipher/internal/protocol"
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
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

type nodeConfig struct {
	connLowWater  int
	connHighWater int
	connGrace     time.Duration
	rcmgr         network.ResourceManager
	rcmgrLimiter  rcmgr.Limiter
}

// HostOption configures custom parameters for NewNode.
type HostOption func(*nodeConfig)

// WithConnLimits sets custom low and high watermarks and grace period for the ConnectionManager.
func WithConnLimits(lowWater, highWater int, gracePeriod time.Duration) HostOption {
	return func(cfg *nodeConfig) {
		cfg.connLowWater = lowWater
		cfg.connHighWater = highWater
		cfg.connGrace = gracePeriod
	}
}

// WithResourceLimits sets custom resource limits or resource manager.
func WithResourceLimits(limits any) HostOption {
	return func(cfg *nodeConfig) {
		switch v := limits.(type) {
		case network.ResourceManager:
			cfg.rcmgr = v
		case rcmgr.Limiter:
			cfg.rcmgrLimiter = v
		case rcmgr.ConcreteLimitConfig:
			cfg.rcmgrLimiter = rcmgr.NewFixedLimiter(v)
		case rcmgr.ScalingLimitConfig:
			scaled := v.AutoScale()
			cfg.rcmgrLimiter = rcmgr.NewFixedLimiter(scaled)
		default:
			log.Printf("[Transport] Warning: unknown resource limit type %T passed to WithResourceLimits", limits)
		}
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, opts ...HostOption) (host.Host, *dht.IpfsDHT, error) {
	nodeCfg := &nodeConfig{
		connLowWater:  100,
		connHighWater: 400,
		connGrace:     1 * time.Minute,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(nodeCfg)
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

	cm, err := connmgr.NewConnManager(
		nodeCfg.connLowWater,
		nodeCfg.connHighWater,
		connmgr.WithGracePeriod(nodeCfg.connGrace),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
	}
	libp2pOpts = append(libp2pOpts, libp2p.ConnectionManager(cm))

	if nodeCfg.rcmgr != nil {
		libp2pOpts = append(libp2pOpts, libp2p.ResourceManager(nodeCfg.rcmgr))
	} else if nodeCfg.rcmgrLimiter != nil {
		rm, err := rcmgr.NewResourceManager(nodeCfg.rcmgrLimiter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		libp2pOpts = append(libp2pOpts, libp2p.ResourceManager(rm))
	} else {
		scalingLimits := rcmgr.DefaultLimits
		libp2p.SetDefaultServiceLimits(&scalingLimits)

		scalingLimits.AddProtocolLimit(
			protocol.FileTransferProtocolID,
			rcmgr.BaseLimit{
				StreamsInbound:  512,
				StreamsOutbound: 2048,
				Streams:         2048,
				Memory:          64 << 20,
			},
			rcmgr.BaseLimitIncrease{
				StreamsInbound:  256,
				StreamsOutbound: 512,
				Streams:         512,
				Memory:          64 << 20,
			},
		)
		scalingLimits.AddProtocolPeerLimit(
			protocol.FileTransferProtocolID,
			rcmgr.BaseLimit{
				StreamsInbound:  64,
				StreamsOutbound: 128,
				Streams:         256,
				Memory:          16 << 20,
			},
			rcmgr.BaseLimitIncrease{
				StreamsInbound:  4,
				StreamsOutbound: 8,
				Streams:         16,
				Memory:          4 << 20,
			},
		)

		scalingLimits.AddProtocolLimit(
			protocol.ChunkTransportProtocolID,
			rcmgr.BaseLimit{
				StreamsInbound:  512,
				StreamsOutbound: 2048,
				Streams:         2048,
				Memory:          64 << 20,
			},
			rcmgr.BaseLimitIncrease{
				StreamsInbound:  256,
				StreamsOutbound: 512,
				Streams:         512,
				Memory:          64 << 20,
			},
		)
		scalingLimits.AddProtocolPeerLimit(
			protocol.ChunkTransportProtocolID,
			rcmgr.BaseLimit{
				StreamsInbound:  64,
				StreamsOutbound: 128,
				Streams:         256,
				Memory:          16 << 20,
			},
			rcmgr.BaseLimitIncrease{
				StreamsInbound:  4,
				StreamsOutbound: 8,
				Streams:         16,
				Memory:          4 << 20,
			},
		)

		scaledLimits := scalingLimits.AutoScale()
		limiter := rcmgr.NewFixedLimiter(scaledLimits)
		rm, err := rcmgr.NewResourceManager(limiter)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		libp2pOpts = append(libp2pOpts, libp2p.ResourceManager(rm))
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
