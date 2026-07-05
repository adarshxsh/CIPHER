package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"

	"cipher-p2p/internal/identity"
)

func main() {
	listenPort := flag.Int("listen", 9002, "Port to listen on")
	keyPath := flag.String("key", "relay.key", "Path to the identity key file")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

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
		listenAddrs = []string{fmt.Sprintf("/ip4/0.0.0.0/tcp/%s/ws", envPort)}
	} else {
		listenAddrs = []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *listenPort),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/ws", *listenPort+1),
		}
	}

	slog.Info("Starting libp2p Relay host", "port", *listenPort, "envPort", envPort)
	host, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.EnableNATService(),
	)
	if err != nil {
		slog.Error("Failed to create libp2p host", "error", err)
		os.Exit(1)
	}
	defer host.Close()

	// Detailed connection/disconnection logging using only slog (no external deps)
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			direction := c.Stat().Direction.String()
			addr := c.RemoteMultiaddr().String()
			isCircuit := strings.Contains(addr, "p2p-circuit")
			slog.Info("Relay: peer connected",
				"peer", c.RemotePeer().String(),
				"addr", addr,
				"direction", direction,
				"is_circuit", isCircuit,
			)
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			slog.Info("Relay: peer disconnected",
				"peer", c.RemotePeer().String(),
				"addr", c.RemoteMultiaddr().String(),
			)
		},
	})

	// Start the Relay service — this is the ONLY relay service.
	// Do NOT also use libp2p.EnableRelayService() on the host above.
	_, err = relay.New(host,
		relay.WithResources(relay.Resources{
			MaxReservations:        256,
			MaxCircuits:            128,
			BufferSize:             4096,
			MaxReservationsPerPeer: 8,
			MaxReservationsPerIP:   16,
		}),
		relay.WithLimit(&relay.RelayLimit{
			Duration: 30 * time.Minute,
			Data:     1 << 30, // 1 GB
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

	// HTTP health endpoint
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			peers := host.Network().Peers()
			conns := host.Network().Conns()
			// Count how many peers have reservations (they show circuit connections)
			circuitCount := 0
			for _, c := range conns {
				if strings.Contains(c.RemoteMultiaddr().String(), "p2p-circuit") {
					circuitCount++
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","peer_id":"%s","peers":%d,"connections":%d,"active_circuits":%d}`,
				host.ID().String(), len(peers), len(conns), circuitCount)
		})
		slog.Info("Health endpoint starting", "port", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			slog.Warn("Health endpoint failed to start (non-fatal)", "error", err)
		}
	}()

	// Periodic detailed status logging
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			peers := host.Network().Peers()
			conns := host.Network().Conns()
			reservedPeers := 0
			for _, c := range conns {
				if len(c.GetStreams()) > 0 {
					reservedPeers++
				}
			}
			slog.Info("Relay status",
				"connected_peers", len(peers),
				"total_connections", len(conns),
				"peers_with_active_streams", reservedPeers,
			)
			// Log each connection with stream count for debugging
			for _, c := range conns {
				slog.Debug("Relay connection detail",
					"peer", c.RemotePeer().String(),
					"addr", c.RemoteMultiaddr().String(),
					"streams", len(c.GetStreams()),
					"direction", c.Stat().Direction.String(),
				)
			}
		}
	}()

	slog.Info("Relay is running. Press Ctrl+C to stop.")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("Shutting down relay...")
}
