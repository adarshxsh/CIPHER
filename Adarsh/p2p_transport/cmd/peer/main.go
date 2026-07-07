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
)

func main() {
	target := flag.String("d", "", "Target peer multiaddress to dial (e.g. /ip4/127.0.0.1/tcp/55555/p2p/Qm...)")
	port := flag.Int("p", 0, "Port to listen on (default 0 for random)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	h, err := transport.NewNode(ctx, *port, priv)
	if err != nil {
		log.Fatalf("Failed to create libp2p node: %v", err)
	}

	// Setup protocol handler
	transport.SetupStreamHandler(h)

	log.Printf("Peer started at %s/p2p/%s", h.Addrs()[0].String(), h.ID().String())

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
