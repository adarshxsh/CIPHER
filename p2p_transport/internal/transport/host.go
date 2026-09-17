package transport

import (
	"cipher/internal/discovery"
	"context"
	"fmt"
	"log"
	"os"

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

// NodeProfile defines the target deployment profile for resource allocation limits.
type NodeProfile string

const (
	ProfileDefault   NodeProfile = "default"
	ProfileBootstrap NodeProfile = "bootstrap"
	ProfileRelay     NodeProfile = "relay"
	ProfileClient    NodeProfile = "client"
)

// ResourceLimitsConfig holds configuration for tailoring libp2p resource manager limits.
type ResourceLimitsConfig struct {
	Profile      NodeProfile               `json:"profile,omitempty"`
	CustomLimits *rcmgr.ConcreteLimitConfig `json:"custom_limits,omitempty"`
	JSONPath     string                    `json:"json_path,omitempty"`
}

type hostOptions struct {
	profile      NodeProfile
	limitsConfig *ResourceLimitsConfig
	customLimits *rcmgr.ConcreteLimitConfig
	jsonPath     string
}

// HostOption configures options for NewNode.
type HostOption func(*hostOptions)

// WithNodeProfile selects a pre-defined resource allocation profile.
func WithNodeProfile(profile NodeProfile) HostOption {
	return func(o *hostOptions) {
		o.profile = profile
	}
}

// WithCustomLimits sets explicit concrete limits for the host resource manager.
func WithCustomLimits(limits rcmgr.ConcreteLimitConfig) HostOption {
	return func(o *hostOptions) {
		o.customLimits = &limits
	}
}

// WithResourceLimitsConfig passes a ResourceLimitsConfig struct.
func WithResourceLimitsConfig(cfg ResourceLimitsConfig) HostOption {
	return func(o *hostOptions) {
		o.limitsConfig = &cfg
	}
}

// WithLimitConfigFile sets a file path to load resource limits JSON from.
func WithLimitConfigFile(path string) HostOption {
	return func(o *hostOptions) {
		o.jsonPath = path
	}
}

// BuildProfileLimits returns a tailored rcmgr.ConcreteLimitConfig for a given NodeProfile.
func BuildProfileLimits(profile NodeProfile) rcmgr.ConcreteLimitConfig {
	var partial rcmgr.PartialLimitConfig

	switch profile {
	case ProfileBootstrap:
		partial = rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				Conns:           10000,
				ConnsInbound:    5000,
				ConnsOutbound:   5000,
				Streams:         50000,
				StreamsInbound:  25000,
				StreamsOutbound: 25000,
				FD:              8192,
				Memory:          4096 << 20, // 4 GB
			},
			PeerDefault: rcmgr.ResourceLimits{
				Conns:           64,
				Streams:         512,
				StreamsInbound:  256,
				StreamsOutbound: 256,
				Memory:          128 << 20, // 128 MB
			},
		}
	case ProfileRelay:
		partial = rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				Conns:           8000,
				ConnsInbound:    4000,
				ConnsOutbound:   4000,
				Streams:         40000,
				StreamsInbound:  20000,
				StreamsOutbound: 20000,
				FD:              8192,
				Memory:          2048 << 20, // 2 GB
			},
			PeerDefault: rcmgr.ResourceLimits{
				Conns:           32,
				Streams:         256,
				StreamsInbound:  128,
				StreamsOutbound: 128,
				Memory:          128 << 20, // 128 MB
			},
		}
	case ProfileClient:
		partial = rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				Conns:           64,
				ConnsInbound:    16,
				ConnsOutbound:   48,
				Streams:         512,
				StreamsInbound:  128,
				StreamsOutbound: 384,
				FD:              256,
				Memory:          128 << 20, // 128 MB
			},
			PeerDefault: rcmgr.ResourceLimits{
				Conns:           8,
				Streams:         128,
				StreamsInbound:  64,
				StreamsOutbound: 64,
				Memory:          32 << 20, // 32 MB
			},
		}
	case ProfileDefault:
		fallthrough
	default:
		partial = rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{
				Conns:           1024,
				ConnsInbound:    512,
				ConnsOutbound:   512,
				Streams:         4096,
				StreamsInbound:  2048,
				StreamsOutbound: 2048,
				FD:              1024,
				Memory:          512 << 20, // 512 MB
			},
			PeerDefault: rcmgr.ResourceLimits{
				Conns:           16,
				Streams:         128,
				StreamsInbound:  64,
				StreamsOutbound: 64,
				Memory:          64 << 20, // 64 MB
			},
		}
	}

	return partial.Build(rcmgr.DefaultLimits.AutoScale())
}

func createResourceManager(opts *hostOptions) (network.ResourceManager, error) {
	var limiter rcmgr.Limiter

	jsonPath := opts.jsonPath
	if jsonPath == "" && opts.limitsConfig != nil {
		jsonPath = opts.limitsConfig.JSONPath
	}

	if jsonPath != "" {
		f, err := os.Open(jsonPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open limits config file: %w", err)
		}
		defer f.Close()

		profile := opts.profile
		if profile == "" && opts.limitsConfig != nil {
			profile = opts.limitsConfig.Profile
		}
		if profile == "" {
			profile = ProfileDefault
		}

		defaults := BuildProfileLimits(profile)
		var errLimiter error
		limiter, errLimiter = rcmgr.NewLimiterFromJSON(f, defaults)
		if errLimiter != nil {
			return nil, fmt.Errorf("failed to parse limits config JSON: %w", errLimiter)
		}
	} else if opts.customLimits != nil {
		limiter = rcmgr.NewFixedLimiter(*opts.customLimits)
	} else if opts.limitsConfig != nil && opts.limitsConfig.CustomLimits != nil {
		limiter = rcmgr.NewFixedLimiter(*opts.limitsConfig.CustomLimits)
	} else {
		profile := opts.profile
		if profile == "" && opts.limitsConfig != nil {
			profile = opts.limitsConfig.Profile
		}
		if profile == "" {
			profile = ProfileDefault
		}
		limits := BuildProfileLimits(profile)
		limiter = rcmgr.NewFixedLimiter(limits)
	}

	return rcmgr.NewResourceManager(limiter)
}

// NewNode creates a new libp2p host configured with resource limits.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, hostOpts ...HostOption) (host.Host, *dht.IpfsDHT, error) {
	var ho hostOptions
	for _, opt := range hostOpts {
		if opt != nil {
			opt(&ho)
		}
	}

	rm, err := createResourceManager(&ho)
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

// NewNodeWithConfig creates a new libp2p host with a ResourceLimitsConfig.
func NewNodeWithConfig(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, cfg ResourceLimitsConfig) (host.Host, *dht.IpfsDHT, error) {
	return NewNode(ctx, listenPort, wsPort, priv, relayAddr, forceRelay, WithResourceLimitsConfig(cfg))
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
