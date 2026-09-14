package discovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// IsRoutableAddress returns true if the multiaddress is valid and publicly routable.
// It returns false if the multiaddress is nil, unspecified (0.0.0.0 or ::),
// loopback (127.0.0.1 or ::1), private IPv4/IPv6, or IPv6 link-local.
// Set CIPHER_ALLOW_LOCAL_IP=1 to allow loopback/private IPs in development or integration testing.
func IsRoutableAddress(a ma.Multiaddr) bool {
	if a == nil {
		return false
	}
	if manet.IsIPUnspecified(a) {
		return false
	}
	if os.Getenv("CIPHER_ALLOW_LOCAL_IP") == "1" {
		return true
	}
	if manet.IsIPLoopback(a) {
		return false
	}
	if manet.IsPrivateAddr(a) {
		return false
	}
	if manet.IsIP6LinkLocal(a) {
		return false
	}
	return true
}

// SanitizeAddresses filters a slice of multiaddresses, returning only routable addresses.
func SanitizeAddresses(addrs []ma.Multiaddr) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}
	var valid []ma.Multiaddr
	for _, a := range addrs {
		if IsRoutableAddress(a) {
			valid = append(valid, a)
		}
	}
	return valid
}

// NewDHT creates and returns a Kademlia DHT bound to the given host.
// It configures default security options: address sanitization filtering,
// public routing table peer filtering, and IP subnet diversity filtering (max 2 per CPL / table).
// Callers can pass additional or overriding dht.Option arguments in opts.
func NewDHT(h host.Host, mode dht.ModeOpt, opts ...dht.Option) (*dht.IpfsDHT, error) {
	dhtOpts := []dht.Option{
		dht.Mode(mode),
	}
	if os.Getenv("CIPHER_ALLOW_LOCAL_IP") != "1" {
		dhtOpts = append(dhtOpts,
			dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
			dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(h, 2, 2)),
		)
	}
	dhtOpts = append(dhtOpts, dht.AddressFilter(SanitizeAddresses))
	dhtOpts = append(dhtOpts, opts...)

	kdht, err := dht.New(h, dhtOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}

	return kdht, nil
}

// Bootstrap connects to a set of known-good peers and runs the DHT's
// self-bootstrap routine so the routing table starts filling up.
func Bootstrap(ctx context.Context, kdht *dht.IpfsDHT, h host.Host, seeds []peer.AddrInfo) error {
	if len(seeds) == 0 {
		return fmt.Errorf("no bootstrap seeds provided")
	}

	connected := 0

	for _, seed := range seeds {
		if seed.ID == h.ID() {
			continue // Skip self
		}

		validAddrs := SanitizeAddresses(seed.Addrs)
		if len(validAddrs) == 0 {
			log.Printf(
				"[DHT] Skipping bootstrap peer %s: no valid routable multiaddresses",
				seed.ID,
			)
			continue
		}

		cleanSeed := peer.AddrInfo{
			ID:    seed.ID,
			Addrs: validAddrs,
		}

		log.Printf(
			"[DHT] Connecting to bootstrap peer %s...",
			cleanSeed.ID,
		)

		if err := h.Connect(ctx, cleanSeed); err != nil {
			log.Printf(
				"[DHT] Failed to connect to bootstrap peer %s: %v",
				cleanSeed.ID,
				err,
			)
			continue
		}

		log.Printf(
			"[DHT] Connected to bootstrap peer %s",
			cleanSeed.ID,
		)

		connected++
	}

	if connected == 0 {
		return fmt.Errorf("failed to connect to any bootstrap seed")
	}

	// Give the newly established connections a moment to settle.
	time.Sleep(500 * time.Millisecond)

	log.Printf("[DHT] Starting DHT bootstrap...")

	// So this is where the control shifts to the DHT's internal bootstrap routine, which will populate the routing table.
	// Below is the flowchart for what this function actually does
	if err := kdht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("DHT bootstrap failed: %w", err)
	}

	log.Printf(
		"[DHT] Bootstrap complete; routing table has %d peers",
		len(kdht.RoutingTable().ListPeers()),
	)

	return nil
}

// kdht.Bootstrap(ctx)
//         │
//         ▼
// Kademlia routing-table refresh
//         │
//         ▼
// query peers
//         │
//         ▼
// learn about peers closer to target
//         │
//         ▼
// A appears as a discovered peer
//         │
//         ▼
// DHT query needs to contact A
//         │
//         ▼
// libp2p DHT query code dials A
//         │
//         ▼
// libp2p Host
//         │
//         ▼
// Network connection established
