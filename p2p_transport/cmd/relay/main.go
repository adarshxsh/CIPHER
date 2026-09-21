package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cipher/internal/identity"
	"cipher/internal/transport"

	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

func main() {
	// Enable libp2p debug logging for circuit v2
	golog.SetLogLevel("relay", "debug")
	golog.SetLogLevel("p2p-circuit", "debug")

	maxDataKB := flag.Int64("max-data-kb", 128, "Maximum data limit per relayed connection in KB")
	maxDurationMin := flag.Int("max-duration-min", 2, "Maximum connection duration limit in minutes")
	maxReservations := flag.Int("max-reservations", 128, "Maximum active relay reservations")
	maxReservationsPerPeer := flag.Int("max-reservations-per-peer", 4, "Maximum reservations per peer")
	port := flag.Int("p", 4001, "Port for the relay to listen on (TCP)")
	wsPort := flag.Int("ws-port", 4004, "Port for the relay to listen on (WebSocket)")
	flag.Parse()

	// Load persistent identity for the relay
	priv, err := identity.LoadOrCreate()
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

	log.Printf("Relay Service Started!")
	log.Printf("Relay Peer ID: %s", h.ID().String())
	log.Printf("Relay Resources: Data Cap=%d KB, Max Duration=%v, Max Reservations=%d, Max Reservations/Peer=%d",
		rc.Limit.Data/1024, rc.Limit.Duration, rc.MaxReservations, rc.MaxReservationsPerPeer)

	fmt.Println("\nRelay Multiaddresses (for other peers to connect):")
	for _, addr := range h.Addrs() {
		fmt.Printf("%s/p2p/%s\n", addr.String(), h.ID().String())
	}
	fmt.Println("\nTo deploy this relay publicly, replace the local IP (e.g., 127.0.0.1 or 192.168.x.x) above with your server's PUBLIC IP.")

	// Wait for termination signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Println("Shutting down relay...")
	if err := h.Close(); err != nil {
		log.Fatalf("Failed to close host: %v", err)
	}
}
