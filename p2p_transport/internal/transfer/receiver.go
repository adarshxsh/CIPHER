package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

// This is actually redundant since we alr have a client.go in the protocol, and this is just an older version of it

// Receive accepts an incoming file transfer from the remote peer.
func Receive(s network.Stream) error {
	if s == nil {
		return ErrNilStream
	}

	if s.Conn() != nil {
		log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())
	}

	// 1. Read Header Metadata
	var header Header
	if err := header.ReadFrom(s); err != nil {
		s.Reset()
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		s.Reset()
		return fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		s.Reset()
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	outPath := filepath.Join(downloadsDir, header.Filename)
	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	outFile, err := os.Create(outPath)
	if err != nil {
		s.Reset()
		return fmt.Errorf("failed to create output file: %w", err)
	}

	cleanup := func(err error) error {
		outFile.Close()
		os.Remove(outPath)
		s.Reset()
		return err
	}

	startTime := time.Now()

	// 3. Receive Data with Progress Tracking and Hashing
	hasher := sha256.New()
	multiWriter := io.MultiWriter(outFile, hasher)

	pr := &progressReader{
		r:     io.LimitReader(s, int64(header.FileSize)),
		total: header.FileSize,
		last:  0,
	}

	received, err := io.Copy(multiWriter, pr)
	if err != nil {
		return cleanup(fmt.Errorf("failed to receive file data: %w", err))
	}

	if uint64(received) != header.FileSize {
		return cleanup(fmt.Errorf("received size mismatch: expected %d, got %d", header.FileSize, received))
	}

	outFile.Close()

	// 4. Read Checksum Trailer
	if err := header.ReadChecksum(s); err != nil {
		return cleanup(fmt.Errorf("failed to read checksum trailer: %w", err))
	}

	duration := time.Since(startTime)
	throughputMB := (float64(received) / (1024 * 1024)) / duration.Seconds()

	// 5. Verify Integrity
	var computedChecksum [32]byte
	copy(computedChecksum[:], hasher.Sum(nil))

	if !bytes.Equal(computedChecksum[:], header.Checksum[:]) {
		log.Printf("[WARNING] Checksum mismatch! Expected %x, got %x", header.Checksum, computedChecksum)
		return cleanup(fmt.Errorf("%w: expected %x, got %x", ErrChecksumMismatch, header.Checksum, computedChecksum))
	}

	s.Close()

	// Determine Connection Type
	connType := "Direct"
	if s.Conn() != nil && s.Conn().RemoteMultiaddr() != nil {
		if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			connType = "Relay"
		}
	}

	log.Printf("\nTransfer Complete (Receiver)")
	log.Printf("Path       : %s", connType)
	log.Printf("Integrity  : VERIFIED")
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}
