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
func Receive(s network.Stream) (err error) {
	if s == nil {
		return fmt.Errorf("stream is nil")
	}

	var outFile *os.File
	var outPath string
	success := false

	defer func() {
		if outFile != nil {
			_ = outFile.Close()
		}
		if !success {
			if outPath != "" {
				_ = os.Remove(outPath)
			}
			_ = s.Reset()
		} else {
			_ = s.Close()
		}
	}()

	remotePeer := "unknown"
	if s.Conn() != nil {
		remotePeer = s.Conn().RemotePeer().String()
	}
	log.Printf("Incoming stream from %s. Preparing to receive...", remotePeer)

	// 1. Read Header
	var header Header
	if err = header.ReadFrom(s); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		return fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
	}

	cleanFilename := filepath.Base(header.Filename)
	if header.Filename == "" || cleanFilename == "." || cleanFilename == ".." || cleanFilename == "/" || len(header.Filename) > MaxFilenameSize {
		return fmt.Errorf("invalid filename: %q", header.Filename)
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err = os.MkdirAll(downloadsDir, 0755); err != nil {
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	outPath = filepath.Join(downloadsDir, cleanFilename)
	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	outFile, err = os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
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
		return fmt.Errorf("failed to receive file data: %w", err)
	}

	if uint64(received) != header.FileSize {
		return fmt.Errorf("received size mismatch: expected %d, got %d", header.FileSize, received)
	}

	duration := time.Since(startTime)
	var throughputMB float64
	if secs := duration.Seconds(); secs > 0 {
		throughputMB = (float64(received) / (1024 * 1024)) / secs
	}

	// 4. Verify Integrity
	var computedChecksum [32]byte
	copy(computedChecksum[:], hasher.Sum(nil))

	if !bytes.Equal(computedChecksum[:], header.Checksum[:]) {
		log.Printf("[WARNING] Checksum mismatch! Expected %x, got %x", header.Checksum, computedChecksum)
		return fmt.Errorf("checksum mismatch: expected %x, got %x", header.Checksum, computedChecksum)
	}

	if err = outFile.Close(); err != nil {
		outFile = nil
		return fmt.Errorf("failed to close output file: %w", err)
	}
	outFile = nil

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

	success = true
	return nil
}
