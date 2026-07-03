package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"cipher-p2p/internal/identity"
	"cipher-p2p/internal/transport"
)

const ProtocolID = "/cipher/filetransfer/1.0.0"

// Timeouts — production-grade defaults.
const (
	// DialTimeout is the maximum time allowed for host.Connect() to establish a connection.
	DialTimeout = 30 * time.Second

	// StreamTimeout is the maximum time allowed for host.NewStream() to open a protocol stream.
	StreamTimeout = 15 * time.Second

	// ReconnectBaseDelay is the initial backoff delay between reconnection attempts.
	ReconnectBaseDelay = 2 * time.Second

	// ReconnectMaxDelay is the maximum backoff delay between reconnection attempts.
	ReconnectMaxDelay = 60 * time.Second

	// ReconnectMaxAttempts is the maximum number of consecutive reconnection attempts (0 = unlimited).
	ReconnectMaxAttempts = 10

	// HealthCheckInterval is how often the health monitor logs active connection state.
	HealthCheckInterval = 10 * time.Second
)

func main() {
	// Parse command-line flags
	listenPort := flag.Int("listen", 0, "Port to listen on (0 for random)")
	targetAddr := flag.String("target", "", "Target peer multiaddress to dial (optional)")
	keyPath := flag.String("key", "identity.key", "Path to the identity key file")
	relayAddrStr := flag.String("relay", "", "Multiaddress of a relay to use (optional)")
	flag.Parse()

	// Configure slog
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Parse Relay Addr if provided
	var relayAddrs []peer.AddrInfo
	if *relayAddrStr != "" {
		maddr, err := multiaddr.NewMultiaddr(*relayAddrStr)
		if err != nil {
			slog.Error("Invalid relay multiaddress", "error", err)
			os.Exit(1)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			slog.Error("Failed to parse relay peer info", "error", err)
			os.Exit(1)
		}
		relayAddrs = append(relayAddrs, *info)
	}

	// 1. Load or Generate Identity Key
	slog.Info("Loading identity key", "path", *keyPath)
	privKey, err := identity.LoadOrGenerateKey(*keyPath)
	if err != nil {
		slog.Error("Failed to load or generate key", "error", err)
		os.Exit(1)
	}

	// 2. Start libp2p Host
	slog.Info("Starting libp2p host", "port", *listenPort)
	h, err := transport.NewHost(privKey, *listenPort, relayAddrs)
	if err != nil {
		slog.Error("Failed to create libp2p host", "error", err)
		os.Exit(1)
	}
	defer h.Close()

	slog.Info("Host started successfully", "peer_id", h.ID().String())
	for _, addr := range h.Addrs() {
		slog.Info("Listening on address", "addr", fmt.Sprintf("%s/p2p/%s", addr, h.ID()))
	}

	// 3. Set up Stream Handler for incoming connections
	h.SetStreamHandler(ProtocolID, func(s network.Stream) {
		handleIncomingStream(h, s)
	})

	// 4. Set up termination signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5. Dial target if provided
	if *targetAddr != "" {
		go func() {
			if err := dialAndExchange(ctx, h, *targetAddr); err != nil {
				slog.Error("Dial and exchange failed", "error", err)
			}
		}()
	}

	// 6. Start health monitor — periodically log active connections
	go healthMonitor(ctx, h)

	// 7. Wait for termination
	slog.Info("Peer is running. Press Ctrl+C to stop.")
	<-sigCh
	cancel()
	slog.Info("Shutting down...")
}

// handleIncomingStream processes an incoming stream on the CIPHER protocol.
func handleIncomingStream(h host.Host, s network.Stream) {
	remotePeer := s.Conn().RemotePeer().String()
	connType := classifyConnection(s.Conn())
	slog.Info("New incoming stream",
		"peer", remotePeer,
		"conn_type", connType,
		"protocol", string(s.Protocol()),
	)

	// Read message from stream
	reader := bufio.NewReader(s)
	msg, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read from stream", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}

	slog.Info("Received message",
		"message", strings.TrimSpace(msg),
		"from", remotePeer,
		"conn_type", connType,
	)

	// Send a reply
	reply := fmt.Sprintf("Hello from %s!\n", h.ID().String())
	_, err = s.Write([]byte(reply))
	if err != nil {
		slog.Error("Failed to write to stream", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}
	s.Close()
}

// dialAndExchange connects to a target peer and performs the hello exchange.
// Uses timeout contexts for both Connect and NewStream — never blocks indefinitely.
// Includes exponential backoff reconnection if the initial dial fails.
func dialAndExchange(ctx context.Context, h host.Host, targetAddrStr string) error {
	slog.Info("Dialing target", "target", targetAddrStr)

	maddr, err := multiaddr.NewMultiaddr(targetAddrStr)
	if err != nil {
		return fmt.Errorf("invalid target multiaddress: %w", err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer info from multiaddress: %w", err)
	}

	// Connect with exponential backoff retry
	if err := connectWithRetry(ctx, h, *info); err != nil {
		return fmt.Errorf("failed to connect after retries: %w", err)
	}

	slog.Info("Connected to peer", "peer_id", info.ID.String())

	// Open a stream with a timeout context — no infinite blocking
	streamCtx, streamCancel := context.WithTimeout(ctx, StreamTimeout)
	defer streamCancel()

	stream, err := h.NewStream(streamCtx, info.ID, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream (timeout=%s): %w", StreamTimeout, err)
	}

	// Send a message
	helloMsg := fmt.Sprintf("Hello from %s!\n", h.ID().String())
	_, err = stream.Write([]byte(helloMsg))
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to write to stream: %w", err)
	}

	// Read reply
	reader := bufio.NewReader(stream)
	reply, err := reader.ReadString('\n')
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to read reply: %w", err)
	}

	slog.Info("Received reply",
		"message", strings.TrimSpace(reply),
		"from", info.ID.String(),
		"conn_type", classifyConnection(h.Network().ConnsToPeer(info.ID)[0]),
	)
	stream.Close()
	return nil
}

// connectWithRetry attempts to connect to a peer with exponential backoff.
// This handles transient failures like Render cold starts (free tier spins down on idle).
func connectWithRetry(ctx context.Context, h host.Host, info peer.AddrInfo) error {
	delay := ReconnectBaseDelay

	for attempt := 1; ReconnectMaxAttempts == 0 || attempt <= ReconnectMaxAttempts; attempt++ {
		dialCtx, dialCancel := context.WithTimeout(ctx, DialTimeout)
		err := h.Connect(dialCtx, info)
		dialCancel()

		if err == nil {
			if attempt > 1 {
				slog.Info("Reconnected successfully", "attempt", attempt, "peer", info.ID.String())
			}
			return nil
		}

		slog.Warn("Connection attempt failed",
			"attempt", attempt,
			"max_attempts", ReconnectMaxAttempts,
			"error", err,
			"next_retry_in", delay.String(),
		)

		// Wait for backoff delay or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Exponential backoff with cap
		delay = time.Duration(math.Min(float64(delay*2), float64(ReconnectMaxDelay)))
	}

	return fmt.Errorf("exhausted %d reconnection attempts to %s", ReconnectMaxAttempts, info.ID.String())
}

// classifyConnection determines whether a connection is Direct or Relayed
// based on the remote multiaddress.
func classifyConnection(conn network.Conn) string {
	maddr := conn.RemoteMultiaddr().String()
	if strings.Contains(maddr, "p2p-circuit") {
		return "Relayed"
	}
	return "Direct"
}

// healthMonitor periodically logs the state of all active connections.
// Provides structured observability into connection health, types, and stream counts.
func healthMonitor(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conns := h.Network().Conns()
			if len(conns) == 0 {
				slog.Debug("Health check: no active connections")
				continue
			}

			for _, c := range conns {
				connType := classifyConnection(c)
				slog.Info("Active Connection",
					"peer", c.RemotePeer().String(),
					"type", connType,
					"addr", c.RemoteMultiaddr().String(),
					"direction", c.Stat().Direction.String(),
					"opened", c.Stat().Opened.Format(time.RFC3339),
					"streams", len(c.GetStreams()),
				)
			}
		}
	}
}
