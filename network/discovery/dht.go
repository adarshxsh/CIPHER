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

// DefaultPublicAddressFilter filters out RFC1918 private and loopback multiaddresses,
// returning only publicly routable multiaddresses.
func DefaultPublicAddressFilter(addrs []ma.Multiaddr) []ma.Multiaddr {
	var publicAddrs []ma.Multiaddr
	for _, addr := range addrs {
		if addr != nil && manet.IsPublicAddr(addr) {
			publicAddrs = append(publicAddrs, addr)
		}
	}
	return publicAddrs
}

type dhtConfig struct {
	publicFilter        bool
	customRTFilter      dht.RouteTableFilterFunc
	customQueryFilter   dht.QueryFilterFunc
	addressFilter       func([]ma.Multiaddr) []ma.Multiaddr
	enablePeerDiversity bool
	maxPerCPL           int
	maxForTable         int
	extraDHTOptions     []dht.Option
}

// DHTOption defines a functional option for configuring DHT initialization.
type DHTOption func(*dhtConfig)

// Option is an alias for DHTOption.
type Option = DHTOption

// WithPublicRoutingFilter enables or disables public routing table and query filters.
func WithPublicRoutingFilter(enable bool) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.publicFilter = enable
	}
}

// WithAddressFilter sets a custom address filtering function run before addresses enter the peerstore.
func WithAddressFilter(filter func([]ma.Multiaddr) []ma.Multiaddr) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.addressFilter = filter
	}
}

// WithPeerDiversityFilter enables peer diversity filtering with specified maximum peers per CPL bucket and table.
func WithPeerDiversityFilter(enable bool, maxPerCPL, maxForTable int) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.enablePeerDiversity = enable
		cfg.maxPerCPL = maxPerCPL
		cfg.maxForTable = maxForTable
	}
}

// WithPrivateRouting explicitly configures private routing filters and disables address filtering for local testing.
func WithPrivateRouting() DHTOption {
	return func(cfg *dhtConfig) {
		cfg.publicFilter = false
		cfg.customRTFilter = dht.PrivateRoutingTableFilter
		cfg.customQueryFilter = dht.PrivateQueryFilter
		cfg.addressFilter = nil
		cfg.enablePeerDiversity = false
	}
}

// WithDHTOptions passes raw libp2p dht.Option options directly to DHT initialization.
func WithDHTOptions(opts ...dht.Option) DHTOption {
	return func(cfg *dhtConfig) {
		cfg.extraDHTOptions = append(cfg.extraDHTOptions, opts...)
	}
}

// NewDHT creates and returns a Kademlia DHT bound to the given host.
// By default in server mode, public routing table filter (dht.PublicRoutingTableFilter),
// query filter (dht.PublicQueryFilter), public address filter (DefaultPublicAddressFilter),
// and routing table peer diversity (dht.NewRTPeerDiversityFilter) are active.
func NewDHT(h host.Host, mode dht.ModeOpt, opts ...DHTOption) (*dht.IpfsDHT, error) {
	allowPrivateEnv := os.Getenv("CIPHER_ALLOW_PRIVATE_DHT") == "1" || os.Getenv("CIPHER_ALLOW_PRIVATE_DHT") == "true"

	cfg := &dhtConfig{
		publicFilter:        !allowPrivateEnv,
		enablePeerDiversity: !allowPrivateEnv,
		maxPerCPL:           2,
		maxForTable:         3,
	}

	if allowPrivateEnv {
		cfg.customRTFilter = dht.PrivateRoutingTableFilter
		cfg.customQueryFilter = dht.PrivateQueryFilter
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	var dhtOpts []dht.Option
	dhtOpts = append(dhtOpts, dht.Mode(mode))

	if cfg.publicFilter {
		if cfg.customRTFilter != nil {
			dhtOpts = append(dhtOpts, dht.RoutingTableFilter(cfg.customRTFilter))
		} else {
			dhtOpts = append(dhtOpts, dht.RoutingTableFilter(dht.PublicRoutingTableFilter))
		}

		if cfg.customQueryFilter != nil {
			dhtOpts = append(dhtOpts, dht.QueryFilter(cfg.customQueryFilter))
		} else {
			dhtOpts = append(dhtOpts, dht.QueryFilter(dht.PublicQueryFilter))
		}

		if cfg.addressFilter != nil {
			dhtOpts = append(dhtOpts, dht.AddressFilter(cfg.addressFilter))
		} else {
			dhtOpts = append(dhtOpts, dht.AddressFilter(DefaultPublicAddressFilter))
		}
	} else {
		if cfg.customRTFilter != nil {
			dhtOpts = append(dhtOpts, dht.RoutingTableFilter(cfg.customRTFilter))
		}
		if cfg.customQueryFilter != nil {
			dhtOpts = append(dhtOpts, dht.QueryFilter(cfg.customQueryFilter))
		}
		if cfg.addressFilter != nil {
			dhtOpts = append(dhtOpts, dht.AddressFilter(cfg.addressFilter))
		}
	}

	if cfg.enablePeerDiversity && h != nil {
		maxCPL := cfg.maxPerCPL
		if maxCPL <= 0 {
			maxCPL = 2
		}
		maxTable := cfg.maxForTable
		if maxTable <= 0 {
			maxTable = 3
		}
		df := dht.NewRTPeerDiversityFilter(h, maxCPL, maxTable)
		if df != nil {
			dhtOpts = append(dhtOpts, dht.RoutingTablePeerDiversityFilter(df))
		}
	}

	if len(cfg.extraDHTOptions) > 0 {
		dhtOpts = append(dhtOpts, cfg.extraDHTOptions...)
	}

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
