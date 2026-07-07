package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cipher/internal/identity"
	"cipher/internal/transport"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	// Start a relay on port 4001
	h, err := transport.NewNode(ctx, 4001, priv)
	if err != nil {
		log.Fatalf("Failed to create libp2p relay node: %v", err)
	}

	log.Printf("Relay started at %s", h.Addrs()[0].String())
	log.Printf("Relay ID: %s", h.ID().String())

	// Wait for termination signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Println("Shutting down relay...")
	if err := h.Close(); err != nil {
		log.Fatalf("Failed to close host: %v", err)
	}
}
