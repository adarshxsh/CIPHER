package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cipher/internal/identity"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load persistent identity for the relay
	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	// Listen on TCP 4001 and UDP 4001 (QUIC)
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		),
		libp2p.Identity(priv),
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
	}

	// Instantiate the circuit v2 relay service
	_, err = relay.New(h)
	if err != nil {
		log.Fatalf("Failed to instantiate relay service: %v", err)
	}

	log.Printf("Relay Service Started!")
	log.Printf("Relay Peer ID: %s", h.ID().String())
	
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
