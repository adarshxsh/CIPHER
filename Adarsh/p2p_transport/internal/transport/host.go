package transport

import (
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NewHost creates a new libp2p host with the given private key.
// By default, it listens on all network interfaces on a random port (if listenPort is 0).
// For Phase 2, we can also pass a list of static relay addresses to connect through.
func NewHost(privKey crypto.PrivKey, listenPort int, relayAddrs []peer.AddrInfo) (host.Host, error) {
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)

	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddr),
		// Phase 2: Enable dialing through relays
		libp2p.EnableRelay(),
		// Phase 3: Enable hole punching (DCUtR) to upgrade relayed connections to direct connections
		// Temporarily disabled: DCUtR is causing 'context deadline exceeded' on NewStream during connection migration.
		// libp2p.EnableHolePunching(),
	}

	// If we have static relays, configure AutoRelay to maintain reservations on them
	if len(relayAddrs) > 0 {
		opts = append(opts, 
			libp2p.EnableAutoRelayWithStaticRelays(relayAddrs),
			// Force the node to believe it is behind a NAT. 
			// Otherwise, when testing locally, it thinks it's publicly reachable and WON'T ask the relay for a reservation!
			libp2p.ForceReachabilityPrivate(),
		)
	}

	// Create the libp2p node
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	return h, nil
}
