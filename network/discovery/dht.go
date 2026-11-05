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

// PublicAddressFilter filters out non-public multiaddresses (loopback, RFC1918 private, link-local).
func PublicAddressFilter(addrs []ma.Multiaddr) []ma.Multiaddr {
	var filtered []ma.Multiaddr
	for _, a := range addrs {
		if manet.IsPublicAddr(a) {
			filtered = append(filtered, a)
		}
	}
	return filtered
}

// NewDHT creates and returns a Kademlia DHT bound to the given host.
// In public swarm mode, it configures standard Kad-DHT security filters including
// PublicRoutingTableFilter, PublicQueryFilter, RoutingTablePeerDiversityFilter,
// and AddressFilter using manet.IsPublicAddr.
func NewDHT(h host.Host, mode dht.ModeOpt, opts ...dht.Option) (*dht.IpfsDHT, error) {
	defaultOpts := []dht.Option{
		dht.Mode(mode),
	}

	allowLocal := os.Getenv("CIPHER_ALLOW_LOCAL") == "1" || os.Getenv("CIPHER_LOCAL_DEV") == "1"

	if !allowLocal {
		defaultOpts = append(defaultOpts,
			dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
			dht.QueryFilter(dht.PublicQueryFilter),
			dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(h, 2, 3)),
			dht.AddressFilter(PublicAddressFilter),
		)
	}

	allOpts := append(defaultOpts, opts...)
	kdht, err := dht.New(h, allOpts...)

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

		log.Printf(
			"[DHT] Connecting to bootstrap peer %s...",
			seed.ID,
		)

		if err := h.Connect(ctx, seed); err != nil {
			log.Printf(
				"[DHT] Failed to connect to bootstrap peer %s: %v",
				seed.ID,
				err,
			)
			continue
		}

		log.Printf(
			"[DHT] Connected to bootstrap peer %s",
			seed.ID,
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
