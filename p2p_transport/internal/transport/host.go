package transport

import (
	"cipher/internal/discovery"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2pconnmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

type nodeConfig struct {
	connLow         int
	connHigh        int
	connGracePeriod time.Duration
	connMgr         connmgr.ConnManager

	rcLimiter   rcmgr.Limiter
	rcOpts      []rcmgr.Option
	resourceMgr network.ResourceManager

	extraLibp2pOpts []libp2p.Option
}

// NodeOption defines functional options for configuring transport nodes.
type NodeOption func(*nodeConfig) error

// WithConnectionLimits configures custom connection manager watermarks and grace period.
func WithConnectionLimits(low, high int, gracePeriod ...time.Duration) NodeOption {
	return func(cfg *nodeConfig) error {
		cfg.connLow = low
		cfg.connHigh = high
		if len(gracePeriod) > 0 {
			cfg.connGracePeriod = gracePeriod[0]
		}
		return nil
	}
}

// WithConnectionManager sets an explicit connection manager instance.
func WithConnectionManager(cm connmgr.ConnManager) NodeOption {
	return func(cfg *nodeConfig) error {
		cfg.connMgr = cm
		return nil
	}
}

// WithResourceLimits configures custom resource manager limiters and options.
func WithResourceLimits(limiter rcmgr.Limiter, opts ...rcmgr.Option) NodeOption {
	return func(cfg *nodeConfig) error {
		cfg.rcLimiter = limiter
		cfg.rcOpts = opts
		return nil
	}
}

// WithResourceManager sets an explicit resource manager instance.
func WithResourceManager(rm network.ResourceManager) NodeOption {
	return func(cfg *nodeConfig) error {
		cfg.resourceMgr = rm
		return nil
	}
}

// WithLibp2pOptions allows passing custom raw libp2p options.
func WithLibp2pOptions(opts ...libp2p.Option) NodeOption {
	return func(cfg *nodeConfig) error {
		cfg.extraLibp2pOpts = append(cfg.extraLibp2pOpts, opts...)
		return nil
	}
}

type rcmgrWarningLogger struct{}

func (l *rcmgrWarningLogger) AllowConn(dir network.Direction, usefd bool) {}
func (l *rcmgrWarningLogger) BlockConn(dir network.Direction, usefd bool) {
	log.Printf("[WARNING] [ResourceManager] Connection blocked: direction=%v, useFD=%t", dir, usefd)
}

func (l *rcmgrWarningLogger) AllowStream(p peer.ID, dir network.Direction) {}
func (l *rcmgrWarningLogger) BlockStream(p peer.ID, dir network.Direction) {
	log.Printf("[WARNING] [ResourceManager] Stream blocked: peer=%s, direction=%v", p, dir)
}

func (l *rcmgrWarningLogger) AllowPeer(p peer.ID) {}
func (l *rcmgrWarningLogger) BlockPeer(p peer.ID) {
	log.Printf("[WARNING] [ResourceManager] Peer connection blocked: peer=%s", p)
}

func (l *rcmgrWarningLogger) AllowProtocol(proto protocol.ID) {}
func (l *rcmgrWarningLogger) BlockProtocol(proto protocol.ID) {
	log.Printf("[WARNING] [ResourceManager] Protocol stream blocked: protocol=%s", proto)
}

func (l *rcmgrWarningLogger) BlockProtocolPeer(proto protocol.ID, p peer.ID) {
	log.Printf("[WARNING] [ResourceManager] Protocol peer stream blocked: protocol=%s, peer=%s", proto, p)
}

func (l *rcmgrWarningLogger) AllowService(svc string) {}
func (l *rcmgrWarningLogger) BlockService(svc string) {
	log.Printf("[WARNING] [ResourceManager] Service stream blocked: service=%s", svc)
}

func (l *rcmgrWarningLogger) BlockServicePeer(svc string, p peer.ID) {
	log.Printf("[WARNING] [ResourceManager] Service peer stream blocked: service=%s, peer=%s", svc, p)
}

func (l *rcmgrWarningLogger) AllowMemory(size int) {}
func (l *rcmgrWarningLogger) BlockMemory(size int) {
	log.Printf("[WARNING] [ResourceManager] Memory reservation blocked: size=%d bytes", size)
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	cfg := &nodeConfig{
		connLow:         100,
		connHigh:        400,
		connGracePeriod: 1 * time.Minute,
	}

	for _, opt := range nodeOpts {
		if err := opt(cfg); err != nil {
			return nil, nil, fmt.Errorf("failed to apply node option: %w", err)
		}
	}

	if cfg.connMgr == nil {
		cm, err := p2pconnmgr.NewConnManager(
			cfg.connLow,
			cfg.connHigh,
			p2pconnmgr.WithGracePeriod(cfg.connGracePeriod),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create connection manager: %w", err)
		}
		cfg.connMgr = cm
	}

	if cfg.resourceMgr == nil {
		if cfg.rcLimiter == nil {
			cfg.rcLimiter = rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale())
		}
		rcOpts := append([]rcmgr.Option{rcmgr.WithMetrics(&rcmgrWarningLogger{})}, cfg.rcOpts...)
		rm, err := rcmgr.NewResourceManager(cfg.rcLimiter, rcOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		cfg.resourceMgr = rm
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
		libp2p.ConnectionManager(cfg.connMgr),
		libp2p.ResourceManager(cfg.resourceMgr),
	}

	opts = append(opts, cfg.extraLibp2pOpts...)

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

	setupNetworkMonitor(h, cfg.connHigh)

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

func setupNetworkMonitor(h host.Host, highWatermark int) {
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

			if highWatermark > 0 && len(n.Conns()) >= highWatermark {
				log.Printf("[WARNING] [ConnManager] Connection watermark reached: active=%d, highWatermark=%d", len(n.Conns()), highWatermark)
			}
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
