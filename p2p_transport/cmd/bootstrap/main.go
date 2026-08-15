package main

import (
	"cipher/internal/identity"
	"cipher/internal/transport"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// flag is used to pass in flags from the command line. Here, we define a flag for the port number.
	port := flag.Int(
		"p",
		4003,
		"Port for the bootstrap node",
	)

	// parse the cmd line args passed
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Note ki if we create 2 bootstrap nodes using the same function, they would have the same priv key, and thus the same peerID, so while testing create 2 bootstrap nodes using different priv keys
	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	h, kdht, err := transport.NewNode(ctx, *port, priv, "", false)
	if err != nil {
		log.Fatalf(
			"Failed to create bootstrap node: %v",
			err,
		)
	}

	defer h.Close()
	defer kdht.Close()

	log.Printf(
		"Bootstrap node started with Peer ID: %s",
		h.ID(),
	)

	log.Println("Bootstrap node listening on:")

	for _, addr := range h.Addrs() {
		fmt.Printf("%s/p2p/%s\n", addr, h.ID())
	}

	log.Println()
	log.Println("Bootstrap node is ready.")
	log.Println("Waiting for peers...")

	// this is just listening for shutdown signals from the OS -- SIGINT is Ctrl+C and SIGTERM is termination req from another program
	ch := make(chan os.Signal, 1)

	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-ch

	log.Println("Shutting down bootstrap node...")
}
