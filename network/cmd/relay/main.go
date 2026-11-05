package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cipher/network/identity"
	"cipher/shared/logger"

	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

func main() {
	identityPath := flag.String("identity", "config/identity/relay.key", "Path to relay identity key file (optional)")
	minConns := flag.Int("min-conns", 100, "Minimum connection watermark for ConnectionManager")
	maxConns := flag.Int("max-conns", 400, "Maximum connection watermark for ConnectionManager")
	memLimitMB := flag.Int("memory-limit-mb", 0, "Memory limit in MB for ResourceManager (0 for autoconfigured caps)")
	flag.Parse()

	golog.SetLogLevel("relay", "warn")
	golog.SetLogLevel("p2p-circuit", "warn")

	log := logger.Relay

	// Load persistent identity for the relay
	var priv libp2pcrypto.PrivKey
	var err error
	if *identityPath != "" {
		priv, err = identity.LoadOrCreateFromPath(*identityPath)
	} else {
		priv, err = identity.LoadOrCreate()
	}
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	cm, err := connmgr.NewConnManager(*minConns, *maxConns, connmgr.WithGracePeriod(1*time.Minute))
	if err != nil {
		log.Fatalf("Failed to create connection manager: %v", err)
	}

	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)
	var concreteLimits rcmgr.ConcreteLimitConfig
	if *memLimitMB > 0 {
		concreteLimits = scalingLimits.Scale(int64(*memLimitMB)*1024*1024, 0)
	} else {
		concreteLimits = scalingLimits.AutoScale()
	}
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(concreteLimits))
	if err != nil {
		log.Fatalf("Failed to create resource manager: %v", err)
	}

	// Listen on TCP 4001, UDP 4002 (QUIC), and TCP 4004 (WebSocket)
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4002/quic-v1",
			"/ip4/0.0.0.0/tcp/4004/ws",
		),
		libp2p.Identity(priv),
		libp2p.EnableNATService(),
		libp2p.ConnectionManager(cm),
		libp2p.ResourceManager(rm),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
	}

	rc := relay.DefaultResources()
	rc.Limit.Data = 512 * 1024 * 1024 // 512 MB data limit per connection
	rc.Limit.Duration = 15 * time.Minute // 15 minute duration limit
	rc.MaxReservations = 100

	_, err = relay.New(h, relay.WithResources(rc))
	if err != nil {
		log.Fatalf("Failed to instantiate relay service: %v", err)
	}

	fields := []logger.Field{
		{Key: "Node Role", Value: "Circuit Relay v2 Service"},
		{Key: "Relay Peer ID", Value: h.ID().String()},
		{Key: "Max Reservations", Value: "100"},
		{Key: "Data Limit", Value: "512 MB / connection"},
		{Key: "", Value: "Relay Multiaddresses (for other peers):"},
	}
	for _, addr := range h.Addrs() {
		fields = append(fields, logger.Field{Key: "", Value: fmt.Sprintf("  %s/p2p/%s", addr.String(), h.ID().String())})
	}

	log.Banner("CIPHER CIRCUIT RELAY V2", fields...)
	log.Success("Relay node is operational and accepting peer reservations")

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Warn("Shutting down relay...")
	if err := h.Close(); err != nil {
		log.Fatalf("Failed to close host: %v", err)
	}
}

