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
	relayclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/multiformats/go-multiaddr"

	"cipher-p2p/internal/identity"
	"cipher-p2p/internal/transport"
)

const ProtocolID = "/cipher/filetransfer/1.0.0"

// Timeouts tuned for cloud WebSocket relay circuits.
const (
	// DialTimeout: time to establish the relay circuit (connect-level).
	DialTimeout = 60 * time.Second

	// StreamTimeout: time for a single NewStream + multistream negotiation.
	// 60s because relay circuits add significant round-trip latency on top of
	// the inner TLS + yamux handshake that runs over the relay pipe.
	StreamTimeout = 60 * time.Second

	// ReservationWait: how long Peer 2 waits after dialing the relay before
	// trying to reach Peer 1. Peer 1 needs ~5-10 s to obtain its relay slot.
	ReservationWait = 12 * time.Second

	// ReconnectBaseDelay is the initial back-off between retries.
	ReconnectBaseDelay = 3 * time.Second

	// ReconnectMaxDelay caps the back-off.
	ReconnectMaxDelay = 30 * time.Second

	// ReconnectMaxAttempts – 0 = unlimited.
	ReconnectMaxAttempts = 0

	// HealthCheckInterval is how often the health monitor fires.
	HealthCheckInterval = 10 * time.Second
)

func main() {
	listenPort := flag.Int("listen", 0, "Port to listen on (0 for random)")
	targetAddr := flag.String("target", "", "Target peer multiaddress to dial (optional)")
	keyPath := flag.String("key", "identity.key", "Path to the identity key file")
	relayAddrStr := flag.String("relay", "", "Multiaddress of a relay to use (optional)")
	sendFile := flag.String("send", "", "Path to a file to send to target (optional)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	// Parse relay addr
	// Only activate AutoRelay (reservation mode) when we are the RECEIVER (no -target flag).
	// The sender only needs EnableRelay() to dial through circuits; requesting a reservation
	// while also dialing through the relay causes a yamux write deadlock.
	var relayAddrs []peer.AddrInfo
	var relayPeerInfo *peer.AddrInfo
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
		relayPeerInfo = info
		if *targetAddr == "" {
			// RECEIVER: activate AutoRelay so we get a reservation and are dialable
			relayAddrs = append(relayAddrs, *info)
		}
	}

	slog.Info("Loading identity key", "path", *keyPath)
	privKey, err := identity.LoadOrGenerateKey(*keyPath)
	if err != nil {
		slog.Error("Failed to load or generate key", "error", err)
		os.Exit(1)
	}

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

	// Register incoming stream handler
	h.SetStreamHandler(ProtocolID, func(s network.Stream) {
		go handleIncomingStream(h, s)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Receiver side: obtain and maintain a relay reservation so the sender can dial us
	// through the relay circuit. AutoRelay is not used because it silently fails on
	// cloud VMs that have a public IP, even when ForceReachabilityPrivate() is set.
	if *targetAddr == "" && relayPeerInfo != nil {
		go maintainRelayReservation(ctx, h, *relayPeerInfo)
	}

	// Sender side: connect once, then provide REPL
	if *targetAddr != "" {
		go runSenderLoop(ctx, cancel, h, *targetAddr, *sendFile, relayPeerInfo)
	}

	go healthMonitor(ctx, h)

	slog.Info("Peer is running. Press Ctrl+C to stop.")
	<-sigCh
	cancel()
	slog.Info("Shutting down...")
}

// maintainRelayReservation connects to the relay, obtains a circuit relay v2
// reservation, logs the relay address the receiver is reachable at, and
// automatically renews the reservation before it expires.
//
// This replaces AutoRelay (EnableAutoRelayWithStaticRelays) which silently
// fails on cloud VMs with public IPs even when ForceReachabilityPrivate() is
// set, because autonat v2 can race and override the forced reachability state.
func maintainRelayReservation(ctx context.Context, h host.Host, relayInfo peer.AddrInfo) {
	slog.Info("Relay reservation manager starting", "relay", relayInfo.ID)

	// Connect to the relay first — client.Reserve() requires an existing connection.
	for {
		connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
		err := h.Connect(connectCtx, relayInfo)
		connectCancel()
		if err == nil {
			break
		}
		slog.Warn("Failed to connect to relay, retrying in 5s...", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	slog.Info("Connected to relay, requesting reservation...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reserveCtx, reserveCancel := context.WithTimeout(ctx, 30*time.Second)
		reservation, err := relayclient.Reserve(reserveCtx, h, relayInfo)
		reserveCancel()

		if err != nil {
			slog.Warn("Relay reservation failed, retrying in 10s...", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}

		slog.Info("✅ Relay reservation obtained!",
			"expires", reservation.Expiration.Format(time.RFC3339),
			"limit_duration", reservation.LimitDuration,
			"limit_data", reservation.LimitData,
		)
		// Log each relay address the receiver is now reachable at —
		// the sender must dial one of these as its -target.
		for _, addr := range reservation.Addrs {
			slog.Info("📡 Receiver reachable via relay circuit",
				"circuit_addr", fmt.Sprintf("%s/p2p-circuit/p2p/%s", addr, h.ID()),
			)
		}

		// Renew 60 seconds before the reservation expires.
		renewIn := time.Until(reservation.Expiration) - 60*time.Second
		if renewIn < 10*time.Second {
			renewIn = 10 * time.Second
		}
		slog.Info("Relay reservation active, will renew", "in", renewIn.Round(time.Second))

		select {
		case <-ctx.Done():
			return
		case <-time.After(renewIn):
			slog.Info("Renewing relay reservation...")
		}
	}
}

// runSenderLoop dials the target once, waits for a confirmed circuit,
// then provides an interactive REPL for sending files.
func runSenderLoop(ctx context.Context, cancel context.CancelFunc, h host.Host, targetAddrStr string, initialFile string, relayInfo *peer.AddrInfo) {
	usingRelay := relayInfo != nil
	maddr, err := multiaddr.NewMultiaddr(targetAddrStr)
	if err != nil {
		slog.Error("Invalid target multiaddress", "error", err)
		return
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		slog.Error("Failed to parse peer info from target address", "error", err)
		return
	}

	// If we are going through a relay, give Peer 1 time to obtain its
	// relay reservation before we knock on the circuit.
	if usingRelay {
		// Give Peer 1 enough time to obtain its relay reservation.
		// Without this wait, Peer 2 dials before the relay has confirmed Peer 1's slot.
		slog.Info("Waiting for Peer 1 relay reservation to be confirmed...", "wait", ReservationWait)
		select {
		case <-time.After(ReservationWait):
		case <-ctx.Done():
			return
		}
		// Pre-connect to the relay so it's in our peerstore; this primes the circuit dial
		if relayInfo != nil {
			relayCtx, relayCancel := context.WithTimeout(ctx, 15*time.Second)
			_ = h.Connect(relayCtx, *relayInfo)
			relayCancel()
		}
	}

	// Connect with unlimited retries until ctx is cancelled
	slog.Info("Connecting to target peer...", "peer_id", info.ID.String(), "addr", targetAddrStr)
	if err := connectWithRetry(ctx, h, *info); err != nil {
		slog.Error("Failed to connect to target — giving up", "error", err)
		return
	}

	connType := "Direct"
	conns := h.Network().ConnsToPeer(info.ID)
	if len(conns) > 0 && strings.Contains(conns[0].RemoteMultiaddr().String(), "p2p-circuit") {
		connType = "Relayed (via Azure WebSocket Relay)"
	}
	slog.Info("✅ Persistent connection established!", "peer_id", info.ID.String(), "conn_type", connType)

	// Send initial file immediately if specified on the CLI
	if initialFile != "" {
		slog.Info("Sending initial file...", "file", initialFile)
		if err := sendFileOverConn(ctx, h, info.ID, initialFile); err != nil {
			slog.Error("Initial file transfer failed", "error", err)
		}
	}

	// Interactive REPL — the connection stays open between transfers
	fmt.Println("\n=======================================================")
	fmt.Println("🚀  CIPHER — Persistent File Transfer")
	fmt.Println("Commands:")
	fmt.Println("  send <filepath>   — transfer a file to the remote peer")
	fmt.Println("  status            — show current connection info")
	fmt.Println("  exit / quit       — shut down")
	fmt.Println("=======================================================\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("CIPHER> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "":
			// nothing
		case line == "exit" || line == "quit":
			cancel()
			return
		case line == "status":
			printStatus(h, info.ID)
		case strings.HasPrefix(line, "send "):
			filePath := strings.TrimSpace(strings.TrimPrefix(line, "send "))
			if filePath == "" {
				fmt.Println("❌ Usage: send <filepath>")
				continue
			}
			if err := sendFileOverConn(ctx, h, info.ID, filePath); err != nil {
				slog.Error("File transfer failed", "error", err)
				// Reconnect if the relay circuit was lost
				slog.Info("Attempting to reconnect for next transfer...")
				if err2 := connectWithRetry(ctx, h, *info); err2 != nil {
					slog.Error("Reconnect failed", "error", err2)
				} else {
					slog.Info("Reconnected — ready for next transfer.")
				}
			}
		default:
			fmt.Println("❌ Unknown command. Try: send <filepath>")
		}
	}
}

// printStatus logs the current connections to the target peer.
func printStatus(h host.Host, peerID peer.ID) {
	conns := h.Network().ConnsToPeer(peerID)
	if len(conns) == 0 {
		fmt.Println("⚠️  No active connection to target peer.")
		return
	}
	for _, c := range conns {
		connType := classifyConnection(c)
		fmt.Printf("Connection: type=%s addr=%s streams=%d\n",
			connType, c.RemoteMultiaddr().String(), len(c.GetStreams()))
	}
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

	// Set a deadline so a misbehaving sender cannot block the handler forever.
	s.SetDeadline(time.Now().Add(10 * time.Minute))
	defer s.SetDeadline(time.Time{})

	reader := bufio.NewReader(s)
	msg, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read header from stream", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}

	header := strings.TrimSpace(msg)

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

		slog.Info("Incoming file transfer",
			"filename", filename,
			"size", humanBytes(fileSize),
			"from", remotePeer,
			"conn_type", connType,
		)

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

		// Signal sender that we are ready
		if _, err := s.Write([]byte("READY\n")); err != nil {
			slog.Error("Failed to send READY signal", "error", err)
			s.Reset()
			return
		}

		start := time.Now()
		n, err := io.CopyN(outFile, reader, fileSize)
		duration := time.Since(start)
		if err != nil && err != io.EOF {
			slog.Error("File transfer interrupted", "error", err, "bytes_received", n)
			s.Reset()
			return
		}

		sec := math.Max(duration.Seconds(), 0.001)
		speedMB := (float64(n) / 1024 / 1024) / sec
		slog.Info("🎉 File received successfully!",
			"saved_to", outPath,
			"bytes", n,
			"duration", duration.Round(time.Millisecond).String(),
			"speed", fmt.Sprintf("%.2f MB/s", speedMB),
		)
		s.Close()
		return
	}

	// Default: echo hello
	slog.Info("Received message", "message", header, "from", remotePeer, "conn_type", connType)
	reply := fmt.Sprintf("Hello from %s!\n", h.ID().String())
	_, err = s.Write([]byte(reply))
	if err != nil {
		slog.Error("Failed to write reply", "error", err, "peer", remotePeer)
		s.Reset()
		return
	}
	s.Close()
}

// sendFileOverConn opens a new multiplexed stream over the EXISTING connection
// and transfers the file. It does NOT re-dial.
func sendFileOverConn(ctx context.Context, h host.Host, peerID peer.ID, filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("cannot stat file %s: %w", filePath, err)
	}
	fileSize := fileInfo.Size()
	filename := filepath.Base(filePath)

	slog.Info("Opening stream for file transfer", "file", filename, "size", humanBytes(fileSize))

	stream, err := openStreamWithRetry(ctx, h, peerID, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	// Set a transfer deadline: 5 min should be generous for most files.
	stream.SetDeadline(time.Now().Add(5 * time.Minute))

	// Send header
	header := fmt.Sprintf("FILE:%s:%d\n", filename, fileSize)
	if _, err := stream.Write([]byte(header)); err != nil {
		stream.Reset()
		return fmt.Errorf("failed to send header: %w", err)
	}

	// Wait for READY
	reader := bufio.NewReader(stream)
	reply, err := reader.ReadString('\n')
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to receive READY signal: %w", err)
	}
	if strings.TrimSpace(reply) != "READY" {
		stream.Reset()
		return fmt.Errorf("unexpected READY reply: %s", reply)
	}

	file, err := os.Open(filePath)
	if err != nil {
		stream.Reset()
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()

	slog.Info("Streaming file bytes...")
	start := time.Now()
	n, err := io.CopyBuffer(stream, file, make([]byte, 65536))
	duration := time.Since(start)
	if err != nil {
		stream.Reset()
		return fmt.Errorf("error while streaming: %w", err)
	}

	sec := math.Max(duration.Seconds(), 0.001)
	speedMB := (float64(n) / 1024 / 1024) / sec
	slog.Info("🚀 File sent successfully!",
		"filename", filename,
		"bytes", n,
		"duration", duration.Round(time.Millisecond).String(),
		"speed", fmt.Sprintf("%.2f MB/s", speedMB),
	)
	return nil
}

// openStreamWithRetry tries to open a stream with a short timeout per attempt.
// We do NOT close the relay circuit on failure — closing it loses the peer's address
// from libp2p's routing table, making recovery impossible. Instead we just retry.
func openStreamWithRetry(ctx context.Context, h host.Host, peerID peer.ID, proto protocol.ID) (network.Stream, error) {
	delay := ReconnectBaseDelay
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		streamCtx, streamCancel := context.WithTimeout(ctx, StreamTimeout)
		stream, err := h.NewStream(streamCtx, peerID, proto)
		streamCancel()

		if err == nil {
			if attempt > 1 {
				slog.Info("Stream opened after retry", "attempt", attempt)
			}
			return stream, nil
		}

		slog.Warn("Stream open failed — will retry",
			"attempt", attempt,
			"error", err,
			"next_retry_in", delay,
		)
		// NOTE: do NOT close the relay circuit here. Closing it removes the
		// peer's address from the peerstore, which causes "no addresses" on the
		// next attempt and makes recovery impossible.

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = time.Duration(math.Min(float64(delay*2), float64(ReconnectMaxDelay)))
	}
}

// connectWithRetry dials info with unlimited retries. Backs off on
// NO_RESERVATION errors so Peer 1 has time to obtain its relay slot.
func connectWithRetry(ctx context.Context, h host.Host, info peer.AddrInfo) error {
	delay := ReconnectBaseDelay
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dialCtx, dialCancel := context.WithTimeout(ctx, DialTimeout)
		err := h.Connect(dialCtx, info)
		dialCancel()

		if err == nil {
			if attempt > 1 {
				slog.Info("Connected successfully", "attempt", attempt, "peer", info.ID)
			}
			return nil
		}

		isNoReservation := strings.Contains(err.Error(), "NO_RESERVATION")
		if isNoReservation {
			slog.Warn("Relay has no reservation for Peer 1 yet — waiting longer...",
				"attempt", attempt,
				"next_retry_in", delay,
			)
		} else {
			slog.Warn("Connection attempt failed",
				"attempt", attempt,
				"error", err,
				"next_retry_in", delay,
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = time.Duration(math.Min(float64(delay*2), float64(ReconnectMaxDelay)))
	}
}

// classifyConnection determines whether a connection is Direct or Relayed.
func classifyConnection(conn network.Conn) string {
	if strings.Contains(conn.RemoteMultiaddr().String(), "p2p-circuit") {
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
				slog.Info("Active Connection",
					"peer", c.RemotePeer().String(),
					"type", classifyConnection(c),
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

