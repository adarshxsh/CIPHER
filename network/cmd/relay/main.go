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
	maxDataKB := flag.Int64("max-data-kb", 128, "Maximum data limit per relayed connection in KB")
	maxDurationMin := flag.Int("max-duration-min", 2, "Maximum connection duration limit in minutes")
	maxReservations := flag.Int("max-reservations", 128, "Maximum active relay reservations")
	maxReservationsPerPeer := flag.Int("max-reservations-per-peer", 4, "Maximum reservations per peer")
	port := flag.Int("p", 4001, "Port for the relay to listen on (TCP)")
	wsPort := flag.Int("ws-port", 4004, "Port for the relay to listen on (WebSocket)")
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

	// Default circuit v2 resources with operator flags support.
	rc := relay.DefaultResources()
	if rc.Limit == nil {
		rc.Limit = relay.DefaultLimit()
	}
	if *maxDataKB > 0 {
		rc.Limit.Data = *maxDataKB * 1024
	}
	if *maxDurationMin > 0 {
		rc.Limit.Duration = time.Duration(*maxDurationMin) * time.Minute
	}
	if *maxReservations > 0 {
		rc.MaxReservations = *maxReservations
	}
	if *maxReservationsPerPeer > 0 {
		rc.MaxReservationsPerPeer = *maxReservationsPerPeer
	}

	ctx := context.Background()
	h, _, err := transport.NewNode(
		ctx,
		*port,
		*wsPort,
		priv,
		"",
		false,
		transport.WithRelayResources(rc),
		transport.WithLibp2pOptions(
			libp2p.ListenAddrStrings(
				fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", *port+1),
			),
			libp2p.EnableNATService(),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
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
