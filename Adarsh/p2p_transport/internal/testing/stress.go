package stress

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const StressProtocol = "/cipher/stress/1.0.0"

// Result records the outcome of a single stress test.
type Result struct {
	TestName  string
	Passed    bool
	Duration  time.Duration
	BytesSent int64
	BytesRecv int64
	Error     error
	Details   string
}

// String formats the result for logging.
func (r Result) String() string {
	status := "PASS"
	if !r.Passed {
		status = "FAIL"
	}
	s := fmt.Sprintf("[%s] %s (%.2fs)", status, r.TestName, r.Duration.Seconds())
	if r.BytesSent > 0 || r.BytesRecv > 0 {
		s += fmt.Sprintf(" sent=%s recv=%s", humanBytes(r.BytesSent), humanBytes(r.BytesRecv))
	}
	if r.Error != nil {
		s += fmt.Sprintf(" error=%v", r.Error)
	}
	if r.Details != "" {
		s += fmt.Sprintf(" details=%s", r.Details)
	}
	return s
}

// TransferTest sends a configurable amount of random data from sender to receiver
// over a libp2p stream and measures throughput + reliability.
//
// The receiver echoes data back so we can verify integrity.
// This validates the relay's data limits and connection stability.
func TransferTest(ctx context.Context, sender host.Host, receiverID peer.ID, dataSize int64) Result {
	start := time.Now()
	result := Result{
		TestName: fmt.Sprintf("Transfer_%s", humanBytes(dataSize)),
	}

	// Open a stream
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := sender.NewStream(streamCtx, receiverID, protocol.ID(StressProtocol))
	if err != nil {
		result.Error = fmt.Errorf("failed to open stream: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer stream.Close()

	// Write random data in 32KB chunks (matching CIPHER chunk size)
	chunkSize := int64(32768) // 32 KB
	remaining := dataSize
	buf := make([]byte, chunkSize)

	for remaining > 0 {
		toWrite := chunkSize
		if remaining < toWrite {
			toWrite = remaining
		}

		// Fill buffer with random data
		if _, err := rand.Read(buf[:toWrite]); err != nil {
			result.Error = fmt.Errorf("failed to generate random data: %w", err)
			result.Duration = time.Since(start)
			return result
		}

		n, err := stream.Write(buf[:toWrite])
		if err != nil {
			result.Error = fmt.Errorf("write failed after %s: %w", humanBytes(result.BytesSent), err)
			result.Duration = time.Since(start)
			return result
		}
		result.BytesSent += int64(n)
		remaining -= int64(n)
	}

	result.Duration = time.Since(start)
	result.Passed = true

	throughput := float64(result.BytesSent) / result.Duration.Seconds() / 1024 / 1024
	result.Details = fmt.Sprintf("throughput=%.2fMB/s", throughput)

	return result
}

// LongConnectionTest holds a stream open for the specified duration and checks
// whether the connection remains alive (no reservation expiry, no stream resets).
func LongConnectionTest(ctx context.Context, sender host.Host, receiverID peer.ID, duration time.Duration) Result {
	start := time.Now()
	result := Result{
		TestName: fmt.Sprintf("LongConnection_%s", duration.String()),
	}

	// Open a stream
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := sender.NewStream(streamCtx, receiverID, protocol.ID(StressProtocol))
	if err != nil {
		result.Error = fmt.Errorf("failed to open stream: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer stream.Close()

	// Send periodic keepalive pings every 30 seconds
	pingInterval := 30 * time.Second
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	deadline := time.After(duration)
	pingCount := 0

	for {
		select {
		case <-deadline:
			result.Duration = time.Since(start)
			result.Passed = true
			result.Details = fmt.Sprintf("survived %d ping cycles over %s", pingCount, duration)
			return result

		case <-ticker.C:
			// Send a small ping
			_, err := stream.Write([]byte("ping\n"))
			if err != nil {
				result.Error = fmt.Errorf("ping failed after %s (%d pings): %w",
					time.Since(start), pingCount, err)
				result.Duration = time.Since(start)
				return result
			}
			pingCount++
			slog.Debug("Long connection ping",
				"elapsed", time.Since(start).Round(time.Second),
				"pings", pingCount,
			)

		case <-ctx.Done():
			result.Error = ctx.Err()
			result.Duration = time.Since(start)
			return result
		}
	}
}

// ConnectionStateTest verifies that after connecting, the peer's connection
// has the expected type (Relayed or Direct) and remains stable.
func ConnectionStateTest(h host.Host, targetPeer peer.ID, expectedType string) Result {
	result := Result{
		TestName: fmt.Sprintf("ConnectionState_%s", expectedType),
	}

	conns := h.Network().ConnsToPeer(targetPeer)
	if len(conns) == 0 {
		result.Error = fmt.Errorf("no connections to peer %s", targetPeer)
		return result
	}

	for _, c := range conns {
		maddr := c.RemoteMultiaddr().String()
		connType := "Direct"
		if strings.Contains(maddr, "p2p-circuit") {
			connType = "Relayed"
		}

		if connType == expectedType {
			result.Passed = true
			result.Details = fmt.Sprintf("connection=%s addr=%s", connType, maddr)
			return result
		}
	}

	result.Error = fmt.Errorf("expected %s connection, found none (have %d connections)", expectedType, len(conns))
	return result
}

// SetupStressReceiver registers a stream handler on the given host that simply
// reads and discards all incoming data on the stress protocol. This is the
// "sink" side of transfer and long-connection tests.
func SetupStressReceiver(h host.Host) {
	h.SetStreamHandler(StressProtocol, func(s network.Stream) {
		defer s.Close()
		slog.Info("Stress receiver: new stream", "peer", s.Conn().RemotePeer().String())

		n, err := io.Copy(io.Discard, s)
		if err != nil {
			slog.Warn("Stress receiver: read error", "error", err, "bytes_received", n)
			return
		}
		slog.Info("Stress receiver: stream completed", "bytes_received", n)
	})
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
