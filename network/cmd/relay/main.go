package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cipher/network/identity"
	"cipher/network/transport"
	"cipher/shared/logger"

	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

func main() {
	identityPath := flag.String("identity", "config/identity/relay.key", "Path to relay identity key file (optional)")
	maxReservations := flag.Int("max-reservations", 128, "Maximum number of active relay reservation slots")
	durationLimit := flag.Duration("duration-limit", 2*time.Minute, "Max stream duration per relayed connection")
	dataLimitKB := flag.Int64("data-limit-kb", 128, "Max transfer data limit in KB per relayed connection")
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

	// Listen on TCP 4001, UDP 4002 (QUIC), and TCP 4004 (WebSocket)
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4002/quic-v1",
			"/ip4/0.0.0.0/tcp/4004/ws",
		),
		libp2p.Identity(priv),
		libp2p.EnableNATService(),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
	}

	// Configure relay resources with bounded caps to prevent bandwidth abuse
	rc := transport.DefaultRelayResources()
	rc.MaxReservations = *maxReservations
	if rc.Limit == nil {
		rc.Limit = &relay.RelayLimit{}
	}
	rc.Limit.Duration = *durationLimit
	rc.Limit.Data = *dataLimitKB * 1024

	// Instantiate the circuit v2 relay service
	_, err = transport.NewRelayService(h, relay.WithResources(rc))
	if err != nil {
		log.Fatalf("Failed to instantiate relay service: %v", err)
	}

	fields := []logger.Field{
		{Key: "Node Role", Value: "Circuit Relay v2 Service"},
		{Key: "Relay Peer ID", Value: h.ID().String()},
		{Key: "Max Reservations", Value: fmt.Sprintf("%d", rc.MaxReservations)},
		{Key: "Data Limit", Value: fmt.Sprintf("%d KB / connection", rc.Limit.Data/1024)},
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
