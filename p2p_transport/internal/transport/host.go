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
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/multiformats/go-multiaddr"
)

// nodeConfig holds custom options for NewNode.
type nodeConfig struct {
	relayResources *relay.Resources
	enableRelaySvc bool
	metricsTracer  relay.MetricsTracer
	libp2pOpts     []libp2p.Option
}

// NodeOption defines a functional option for configuring NewNode.
type NodeOption func(*nodeConfig)

// WithRelayResources enables the circuitv2 relay service with specified resources.
func WithRelayResources(rc relay.Resources) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.relayResources = &rc
		cfg.enableRelaySvc = true
	}
}

// WithRelayService enables the circuitv2 relay service with specified resources.
func WithRelayService(rc relay.Resources) NodeOption {
	return WithRelayResources(rc)
}

// WithRelayMetricsTracer sets a custom metrics tracer for the relay service.
func WithRelayMetricsTracer(mt relay.MetricsTracer) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.metricsTracer = mt
	}
}

// WithLibp2pOptions appends custom libp2p.Option values to host instantiation.
func WithLibp2pOptions(opts ...libp2p.Option) NodeOption {
	return func(cfg *nodeConfig) {
		cfg.libp2pOpts = append(cfg.libp2pOpts, opts...)
	}
}

// relayWarningTracer wraps or implements MetricsTracer to log warnings when limits/quotas are exceeded.
type relayWarningTracer struct {
	inner relay.MetricsTracer
}

func newRelayWarningTracer(inner relay.MetricsTracer) *relayWarningTracer {
	return &relayWarningTracer{inner: inner}
}

func (t *relayWarningTracer) RelayStatus(enabled bool) {
	if t.inner != nil {
		t.inner.RelayStatus(enabled)
	}
}

func (t *relayWarningTracer) ConnectionOpened() {
	if t.inner != nil {
		t.inner.ConnectionOpened()
	}
}

func (t *relayWarningTracer) ConnectionClosed(d time.Duration) {
	if t.inner != nil {
		t.inner.ConnectionClosed(d)
	}
}

func (t *relayWarningTracer) ConnectionRequestHandled(st pbv2.Status) {
	if st != pbv2.Status_OK {
		log.Printf("[Relay Warning] Connection request rejected or exceeded limit: status=%s", st)
	}
	if t.inner != nil {
		t.inner.ConnectionRequestHandled(st)
	}
}

func (t *relayWarningTracer) ReservationAllowed(isRenewal bool) {
	if t.inner != nil {
		t.inner.ReservationAllowed(isRenewal)
	}
}

func (t *relayWarningTracer) ReservationClosed(cnt int) {
	if t.inner != nil {
		t.inner.ReservationClosed(cnt)
	}
}

func (t *relayWarningTracer) ReservationRequestHandled(st pbv2.Status) {
	if st != pbv2.Status_OK {
		log.Printf("[Relay Warning] Reservation request rejected or exceeded quota: status=%s", st)
	}
	if t.inner != nil {
		t.inner.ReservationRequestHandled(st)
	}
}

func (t *relayWarningTracer) BytesTransferred(cnt int) {
	if t.inner != nil {
		t.inner.BytesTransferred(cnt)
	}
}

// NewNode creates a new libp2p host.
func NewNode(ctx context.Context, listenPort int, wsPort int, priv crypto.PrivKey, relayAddr string, forceRelay bool, nodeOpts ...NodeOption) (host.Host, *dht.IpfsDHT, error) {
	var nodeCfg nodeConfig
	for _, opt := range nodeOpts {
		opt(&nodeCfg)
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

	if len(nodeCfg.libp2pOpts) > 0 {
		opts = append(opts, nodeCfg.libp2pOpts...)
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	if nodeCfg.enableRelaySvc || nodeCfg.relayResources != nil {
		rc := relay.DefaultResources()
		if nodeCfg.relayResources != nil {
			rc = *nodeCfg.relayResources
		}
		tracer := newRelayWarningTracer(nodeCfg.metricsTracer)
		_, err = relay.New(h, relay.WithResources(rc), relay.WithMetricsTracer(tracer))
		if err != nil {
			h.Close()
			return nil, nil, fmt.Errorf("failed to instantiate relay service: %w", err)
		}
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
