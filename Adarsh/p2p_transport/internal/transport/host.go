package transport

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

// Note: RelayAddrs in HostConfig is retained for API compatibility but AutoRelay
// (EnableAutoRelayWithStaticRelays / ForceReachabilityPrivate) has been removed.
// Relay reservations are now obtained explicitly via maintainRelayReservation in
// cmd/peer/main.go using the circuitv2 relay client directly, which is far more
// reliable than AutoRelay on cloud VMs that have a public IP.

// HostConfig contains all configuration options for creating a libp2p host.
// This separates configuration from construction, making the host testable and composable.
type HostConfig struct {
	PrivKey    crypto.PrivKey
	ListenPort int
	RelayAddrs []peer.AddrInfo

	// Connection manager thresholds
	ConnMgrLo    int           // Low watermark — connections below this are never pruned
	ConnMgrHi    int           // High watermark — connections above this are pruned
	ConnMgrGrace time.Duration // Grace period — new connections are immune for this long
}

// DefaultHostConfig returns a HostConfig with sensible defaults for demo/development use.
func DefaultHostConfig(privKey crypto.PrivKey, listenPort int, relayAddrs []peer.AddrInfo) HostConfig {
	return HostConfig{
		PrivKey:      privKey,
		ListenPort:   listenPort,
		RelayAddrs:   relayAddrs,
		ConnMgrLo:    1,
		ConnMgrHi:    10,
		ConnMgrGrace: time.Minute,
	}
}

// NewHost creates a new libp2p host with the given private key.
// By default, it listens on all network interfaces on a random port (if listenPort is 0).
// For Phase 2, we can also pass a list of static relay addresses to connect through.
func NewHost(privKey crypto.PrivKey, listenPort int, relayAddrs []peer.AddrInfo) (host.Host, error) {
	cfg := DefaultHostConfig(privKey, listenPort, relayAddrs)
	return NewHostFromConfig(cfg)
}

// NewHostFromConfig creates a libp2p host from the given HostConfig.
// This is the primary constructor for production use.
func NewHostFromConfig(cfg HostConfig) (host.Host, error) {
	// Build the connection manager to prune idle connections and maintain a healthy peer count.
	cm, err := connmgr.NewConnManager(
		cfg.ConnMgrLo,
		cfg.ConnMgrHi,
		connmgr.WithGracePeriod(cfg.ConnMgrGrace),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort)

	opts := []libp2p.Option{
		libp2p.Identity(cfg.PrivKey),
		libp2p.ListenAddrStrings(listenAddr),

		// Phase 2: Enable dialing through relays
		libp2p.EnableRelay(),

		// Connection manager: prune idle connections, keep the peer table healthy
		libp2p.ConnectionManager(cm),

		// Keepalive: the ping protocol detects dead connections so we don't hold stale state
		libp2p.Ping(true),

		// NOTE: Do NOT specify libp2p.DefaultTransports explicitly.
		// When no transport option is set, libp2p.New() automatically includes
		// TCP, WebSocket, QUIC, AND the relay circuit transport.
		// Explicitly setting DefaultTransports can interfere with the relay circuit
		// transport setup and prevent AutoRelay from obtaining reservations.
	}

	// AutoRelay (EnableAutoRelayWithStaticRelays + ForceReachabilityPrivate) has been
	// intentionally removed. On cloud VMs with public IPs, AutoRelay silently refuses
	// to request a reservation even with ForceReachabilityPrivate(), resulting in
	// persistent streams=0 and NO_RESERVATION errors on the dialer side.
	//
	// Relay reservations are now managed explicitly by maintainRelayReservation() in
	// cmd/peer/main.go using client.Reserve() from the circuitv2 relay client package.
	_ = cfg.RelayAddrs // kept in config for API compatibility

	// Create the libp2p node
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	return h, nil
}
