package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
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
	// NOTE: We intentionally do NOT use libp2p.EnableRelayService() here.
	// Instead, we call relay.New(host, ...) below with custom resource limits.
	// Using both creates TWO relay services — reservations on one are invisible to the other,
	// causing NO_RESERVATION errors when peers try to dial through the circuit.
	slog.Info("Starting libp2p Relay host", "port", *listenPort, "envPort", envPort)
	host, err := libp2p.New(
		libp2p.Identity(privKey),
		// Listen on the configured addresses
		libp2p.ListenAddrStrings(listenAddrs...),
		// Enable AutoNAT service so peers can discover their public addresses
		libp2p.EnableNATService(),
	)
	if err != nil {
		slog.Error("Failed to create libp2p host", "error", err)
		os.Exit(1)
	}
	defer host.Close()

	// Enable deep debug logging for relay subsystems
	golog.SetLogLevel("relay", "debug")
	golog.SetLogLevel("p2p-circuit", "debug")
	golog.SetLogLevel("autorelay", "debug")

	// Add detailed connection logging
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			slog.Info("Relay accepted connection",
				"remote_peer", c.RemotePeer().String(),
				"remote_addr", c.RemoteMultiaddr().String(),
				"direction", c.Stat().Direction.String(),
			)
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			slog.Info("Relay lost connection",
				"remote_peer", c.RemotePeer().String(),
				"remote_addr", c.RemoteMultiaddr().String(),
			)
		},
	})

	// Start the Relay service with demo-grade resource limits.
	// This is the ONLY relay service — do not also use libp2p.EnableRelayService().
	// These limits are intentionally generous for development and demonstration.
	// For production, migrate to a dedicated VPS and tune these down.
	_, err = relay.New(host,
		relay.WithResources(relay.Resources{
			MaxReservations:        256,  // Max peers that can hold reservations
			MaxCircuits:            128,  // Max simultaneous active circuits
			BufferSize:             4096, // 4 KB read buffer per circuit (keeps WSS latency low and prevents NGINX buffer stalling on small multistream packets!)
			MaxReservationsPerPeer: 8,    // Reservations a single peer can hold
			MaxReservationsPerIP:   16,   // Reservations from a single IP
		}),
		relay.WithLimit(&relay.RelayLimit{
			Duration: 30 * time.Minute, // Per-circuit lifetime (30m for large file transfers)
			Data:     1 << 30,          // 1 GB per circuit (up from 128 MB)
		}),
	)
	if err != nil {
		slog.Error("Failed to instantiate relay service", "error", err)
		os.Exit(1)
	}

	slog.Info("Relay started successfully",
		"peer_id", host.ID().String(),
		"max_reservations", 256,
		"max_circuits", 128,
		"buffer_size", "4KB",
		"circuit_duration", "30m",
		"circuit_data_limit", "1GB",
	)
	for _, addr := range host.Addrs() {
		slog.Info("Relay listening on address", "addr", fmt.Sprintf("%s/p2p/%s", addr, host.ID()))
	}

	// 3. Start HTTP health endpoint for monitoring (Render health checks, etc.)
	// This runs on a separate port so it doesn't conflict with libp2p's transport.
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			peerCount := len(host.Network().Peers())
			connCount := len(host.Network().Conns())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","peer_id":"%s","peers":%d,"connections":%d}`,
				host.ID().String(), peerCount, connCount)
		})
		slog.Info("Health endpoint starting", "port", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			slog.Warn("Health endpoint failed to start (non-fatal)", "error", err)
		}
	}()

	// 4. Periodic connection logging
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			peers := host.Network().Peers()
			slog.Info("Relay status",
				"connected_peers", len(peers),
				"total_connections", len(host.Network().Conns()),
			)
		}
	}()

	// 5. Wait for termination signal
	slog.Info("Relay is running. Press Ctrl+C to stop.")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down relay...")
}
