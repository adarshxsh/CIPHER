package main

import (
	"context"
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

	ctx := context.Background()

	// Instantiate relay host via transport.NewNode with connection and resource manager limits configured
	h, kdht, err := transport.NewNode(
		ctx,
		4001,
		4004,
		priv,
		"",
		false,
		transport.WithConnectionLimits(100, 400, 1*time.Minute),
		transport.WithLibp2pOptions(
			libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/4002/quic-v1"),
			libp2p.EnableNATService(),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
	}
	if kdht != nil {
		defer kdht.Close()
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

