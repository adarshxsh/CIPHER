package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	StreamTimeout = 45 * time.Second

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
	sendFile := flag.String("send", "", "Path to a file to send to target (optional)")
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
			if *sendFile != "" {
				if err := sendFileToTarget(ctx, h, *targetAddr, *sendFile); err != nil {
					slog.Error("File transfer failed", "error", err)
				}
			} else {
				if err := dialAndExchange(ctx, h, *targetAddr); err != nil {
					slog.Error("Dial and exchange failed", "error", err)
				}
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

	reader := bufio.NewReader(s)
	msg, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read header from stream", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}

	header := strings.TrimSpace(msg)

	// Check if this is a file transfer request: FILE:<filename>:<size>
	if strings.HasPrefix(header, "FILE:") {
		parts := strings.Split(header, ":")
		if len(parts) != 3 {
			slog.Error("Invalid file transfer header", "header", header)
			s.Reset()
			return
		}
		filename := filepath.Base(parts[1])
		fileSize, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			slog.Error("Invalid file size in header", "size", parts[2], "error", err)
			s.Reset()
			return
		}

		slog.Info("Incoming file transfer request",
			"filename", filename,
			"size", humanBytes(fileSize),
			"from", remotePeer,
			"conn_type", connType,
		)

		// Create output directory
		outDir := "received"
		if err := os.MkdirAll(outDir, 0755); err != nil {
			slog.Error("Failed to create received directory", "error", err)
			s.Reset()
			return
		}

		outPath := filepath.Join(outDir, filename)
		outFile, err := os.Create(outPath)
		if err != nil {
			slog.Error("Failed to create output file", "path", outPath, "error", err)
			s.Reset()
			return
		}
		defer outFile.Close()

		// Send READY signal to sender
		if _, err := s.Write([]byte("READY\n")); err != nil {
			slog.Error("Failed to send READY signal", "error", err)
			s.Reset()
			return
		}

		// Receive data
		start := time.Now()
		n, err := io.CopyN(outFile, reader, fileSize)
		duration := time.Since(start)
		if err != nil && err != io.EOF {
			slog.Error("File transfer interrupted", "error", err, "bytes_received", n)
			s.Reset()
			return
		}

		speedMB := (float64(n) / 1024 / 1024) / duration.Seconds()
		slog.Info("🎉 File received successfully!",
			"saved_to", outPath,
			"bytes", n,
			"duration", duration.Round(time.Millisecond).String(),
			"speed", fmt.Sprintf("%.2f MB/s", speedMB),
		)
		s.Close()
		return
	}

	// Default hello message handling
	slog.Info("Received message",
		"message", header,
		"from", remotePeer,
		"conn_type", connType,
	)

	reply := fmt.Sprintf("Hello from %s!\n", h.ID().String())
	_, err = s.Write([]byte(reply))
	if err != nil {
		slog.Error("Failed to write reply", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}
	s.Close()
}

// sendFileToTarget sends a local file over a stream to the target peer.
func sendFileToTarget(ctx context.Context, h host.Host, targetAddrStr string, filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("cannot read file %s: %w", filePath, err)
	}
	fileSize := fileInfo.Size()
	filename := filepath.Base(filePath)

	slog.Info("Preparing to send file", "file", filePath, "size", humanBytes(fileSize), "target", targetAddrStr)

	maddr, err := multiaddr.NewMultiaddr(targetAddrStr)
	if err != nil {
		return fmt.Errorf("invalid target multiaddress: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("failed to parse peer info: %w", err)
	}

	if err := connectWithRetry(ctx, h, *info); err != nil {
		return fmt.Errorf("failed to connect after retries: %w", err)
	}

	stream, err := openStreamWithRetry(ctx, h, info.ID, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}

	// Send file header
	header := fmt.Sprintf("FILE:%s:%d\n", filename, fileSize)
	if _, err := stream.Write([]byte(header)); err != nil {
		stream.Reset()
		return fmt.Errorf("failed to send header: %w", err)
	}

	// Wait for READY reply
	reader := bufio.NewReader(stream)
	reply, err := reader.ReadString('\n')
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to receive READY signal: %w", err)
	}
	if strings.TrimSpace(reply) != "READY" {
		stream.Reset()
		return fmt.Errorf("unexpected reply from receiver: %s", reply)
	}

	// Open file and stream bytes
	file, err := os.Open(filePath)
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	slog.Info("Streaming file bytes across peer connection...")
	start := time.Now()
	// Use a 64KB buffer for copying to reduce WebSocket framing overhead across cloud relays
	n, err := io.CopyBuffer(stream, file, make([]byte, 65536))
	duration := time.Since(start)
	if err != nil {
		stream.Reset()
		return fmt.Errorf("error while streaming file: %w", err)
	}

	speedMB := (float64(n) / 1024 / 1024) / duration.Seconds()
	slog.Info("🚀 File sent successfully!",
		"filename", filename,
		"bytes", n,
		"duration", duration.Round(time.Millisecond).String(),
		"speed", fmt.Sprintf("%.2f MB/s", speedMB),
	)
	stream.Close()
	return nil
}

// dialAndExchange connects to a target peer and performs the hello exchange.
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

	if err := connectWithRetry(ctx, h, *info); err != nil {
		return fmt.Errorf("failed to connect after retries: %w", err)
	}

	slog.Info("Connected to peer", "peer_id", info.ID.String())

	stream, err := openStreamWithRetry(ctx, h, info.ID, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream (timeout=%s): %w", StreamTimeout, err)
	}

	helloMsg := fmt.Sprintf("Hello from %s!\n", h.ID().String())
	_, err = stream.Write([]byte(helloMsg))
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to write to stream: %w", err)
	}

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

// openStreamWithRetry attempts to open a stream with exponential backoff retries.
// Over cloud relay circuits (like Render WSS), multistream negotiation can occasionally
// experience transient latency or timeouts.
func openStreamWithRetry(ctx context.Context, h host.Host, peerID peer.ID, proto protocol.ID) (network.Stream, error) {
	delay := ReconnectBaseDelay
	for attempt := 1; ReconnectMaxAttempts == 0 || attempt <= ReconnectMaxAttempts; attempt++ {
		streamCtx, streamCancel := context.WithTimeout(ctx, StreamTimeout)
		stream, err := h.NewStream(streamCtx, peerID, proto)
		streamCancel()

		if err == nil {
			if attempt > 1 {
				slog.Info("Stream opened successfully after retry", "attempt", attempt, "peer", peerID.String())
			}
			return stream, nil
		}

		slog.Warn("Stream opening attempt failed",
			"attempt", attempt,
			"max_attempts", ReconnectMaxAttempts,
			"error", err,
			"next_retry_in", delay.String(),
		)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		delay = time.Duration(math.Min(float64(delay*2), float64(ReconnectMaxDelay)))
	}
	return nil, fmt.Errorf("exhausted %d stream opening attempts to %s", ReconnectMaxAttempts, peerID.String())
}

// connectWithRetry attempts to connect to a peer with exponential backoff.
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = time.Duration(math.Min(float64(delay*2), float64(ReconnectMaxDelay)))
	}

	return fmt.Errorf("exhausted %d reconnection attempts to %s", ReconnectMaxAttempts, info.ID.String())
}

// classifyConnection determines whether a connection is Direct or Relayed.
func classifyConnection(conn network.Conn) string {
	maddr := conn.RemoteMultiaddr().String()
	if strings.Contains(maddr, "p2p-circuit") {
		return "Relayed"
	}
	return "Direct"
}

// healthMonitor periodically logs the state of all active connections.
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

// humanBytes converts a byte count to a human-readable string.
func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%s", float64(b)/float64(div), []string{"KB", "MB", "GB", "TB"}[exp])
}
