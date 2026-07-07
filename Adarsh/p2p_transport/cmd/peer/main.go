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
	"cipher/internal/transfer"
	"cipher/internal/transport"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	golog "github.com/ipfs/go-log/v2"
)

func main() {
	// Enable libp2p debug logging for circuit v2 and identify
	golog.SetLogLevel("relay", "debug")
	golog.SetLogLevel("autorelay", "debug")
	golog.SetLogLevel("p2p-circuit", "debug")
	golog.SetLogLevel("identify", "debug")
	target := flag.String("d", "", "Target peer multiaddress to dial (e.g. /ip4/127.0.0.1/tcp/55555/p2p/Qm...)")
	port := flag.Int("p", 0, "Port to listen on (default 0 for random)")
	relayAddr := flag.String("relay", "", "Static relay multiaddress to use for NAT traversal")
	sendFile := flag.String("send", "", "Path to the file you want to send to the target peer")
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
				if res, err := client.Reserve(ctx, h, *relayInfo); err != nil {
					log.Printf("Warning: Failed to reserve slot on relay: %v", err)
				} else {
					log.Printf("\n[✓] Successfully connected to relay and reserved slot!")
					log.Printf("    Reservation Expiration: %s", res.Expiration.String())
					log.Printf("    Relay Peer ID: %s", relayInfo.ID.String())
					log.Printf("Your Relayed Multiaddress (Share this with peers to connect to you):")
					log.Printf("  - %s/p2p-circuit/p2p/%s\n", *relayAddr, h.ID().String())
				}
			}
		}
	}

	if *target != "" && *sendFile != "" {
		log.Printf("Dialing target peer: %s", *target)
		t := transport.NewTransport(h)
		
		addrInfo, err := t.Connect(ctx, *target)
		if err != nil {
			log.Fatalf("Failed to connect to target: %v", err)
		}
		
		s, err := t.OpenStream(ctx, addrInfo)
		if err != nil {
			log.Fatalf("Failed to open stream to target: %v", err)
		}
		
		if err := transfer.Send(s, *sendFile); err != nil {
			log.Fatalf("Failed to send file: %v", err)
		}
	} else if *target != "" && *sendFile == "" {
		log.Fatalf("Target peer specified but no file to send. Please use the -send flag.")
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
