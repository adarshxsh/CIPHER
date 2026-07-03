package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"cipher-p2p/internal/identity"
)

func main() {
	// Parse command-line flags
	listenPort := flag.Int("listen", 9002, "Port to listen on")
	keyPath := flag.String("key", "relay.key", "Path to the identity key file")
	flag.Parse()

	// Configure slog
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// 1. Load or Generate Identity Key for Relay
	slog.Info("Loading relay identity key", "path", *keyPath)
	privKey, err := identity.LoadOrGenerateKey(*keyPath)
	if err != nil {
		slog.Error("Failed to load or generate key", "error", err)
		os.Exit(1)
	}

	// Check if PORT environment variable is set (used by Render, Heroku, etc.)
	envPort := os.Getenv("PORT")
	var listenAddrs []string
	if envPort != "" {
		// On cloud providers like Render, ONLY listen on WebSockets to avoid port binding conflicts on $PORT
		listenAddrs = []string{fmt.Sprintf("/ip4/0.0.0.0/tcp/%s/ws", envPort)}
	} else {
		// For local testing, listen on both TCP and WS but on DIFFERENT ports to avoid conflicts!
		listenAddrs = []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *listenPort),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", *listenPort+1),
		}
	}

	// 2. Start libp2p Host
	slog.Info("Starting libp2p Relay host", "port", *listenPort, "envPort", envPort)
	host, err := libp2p.New(
		libp2p.Identity(privKey),
		// Listen on the configured addresses
		libp2p.ListenAddrStrings(listenAddrs...),
		// Enable relay service functionality!
		libp2p.EnableRelayService(),
		// Phase 3: Enable AutoNAT service so peers can discover their public addresses
		libp2p.EnableNATService(),
	)
	if err != nil {
		slog.Error("Failed to create libp2p host", "error", err)
		os.Exit(1)
	}
	defer host.Close()

	// Optionally start the Relay service explicitly if we need custom config,
	// but libp2p.EnableRelayService() already injects it into the host.
	// For reference, circuitv2/relay is enabled by the host option above.
	_, err = relay.New(host)
	if err != nil {
		slog.Error("Failed to instantiate relay service", "error", err)
		os.Exit(1)
	}

	slog.Info("Relay started successfully", "peer_id", host.ID().String())
	for _, addr := range host.Addrs() {
		slog.Info("Relay listening on address", "addr", fmt.Sprintf("%s/p2p/%s", addr, host.ID()))
	}

	// 3. Wait for termination signal
	slog.Info("Relay is running. Press Ctrl+C to stop.")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down relay...")
}
