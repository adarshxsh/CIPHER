package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cipher/internal/identity"
	"cipher/internal/transport"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
)

func main() {
	target := flag.String("d", "", "Target peer multiaddress to dial (e.g. /ip4/127.0.0.1/tcp/55555/p2p/Qm...)")
	port := flag.Int("p", 0, "Port to listen on (default 0 for random)")
	relayAddr := flag.String("relay", "", "Static relay multiaddress to use for NAT traversal")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	h, err := transport.NewNode(ctx, *port, priv, *relayAddr)
	if err != nil {
		log.Fatalf("Failed to create libp2p node: %v", err)
	}

	// Setup protocol handler
	transport.SetupStreamHandler(h)

	log.Printf("Peer initialized with ID: %s", h.ID().String())
	log.Println("Listening on the following local addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  - %s/p2p/%s", addr.String(), h.ID().String())
	}

	if *relayAddr != "" {
		relayInfo, err := peer.AddrInfoFromString(*relayAddr)
		if err == nil {
			// Proactively connect and explicitly reserve a slot on the relay
			if err := h.Connect(ctx, *relayInfo); err != nil {
				log.Printf("Warning: Failed to connect to relay: %v", err)
			} else {
				if _, err := client.Reserve(ctx, h, *relayInfo); err != nil {
					log.Printf("Warning: Failed to reserve slot on relay: %v", err)
				} else {
					log.Printf("\n[✓] Successfully connected to relay and reserved slot!")
					log.Printf("Your Relayed Multiaddress (Share this with peers to connect to you):")
					log.Printf("  - %s/p2p-circuit/p2p/%s\n", *relayAddr, h.ID().String())
				}
			}
		}
	}

	if *target != "" {
		log.Printf("Dialing target peer: %s", *target)
		if err := transport.ConnectAndSayHello(ctx, h, *target); err != nil {
			log.Fatalf("Failed to connect to target: %v", err)
		}
	}

	// Wait for termination signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	fmt.Println()
	log.Println("Shutting down peer...")
	if err := h.Close(); err != nil {
		log.Fatalf("Failed to close host: %v", err)
	}
}
