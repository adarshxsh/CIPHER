package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

// This is actually redundant since we alr have a client.go in the protocol, and this is just an older version of it

// Receive accepts an incoming file transfer from the remote peer.
func Receive(s network.Stream) error {
	defer s.Close()

	log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())

	// 1. Read Header
	var header Header
	if err := header.ReadFrom(s); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		return fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	sanitizedFilename := filepath.Base(header.Filename)
	if sanitizedFilename == "" || sanitizedFilename == "." || sanitizedFilename == "/" || sanitizedFilename == "\\" || sanitizedFilename == ".." {
		return fmt.Errorf("invalid header filename: %q", header.Filename)
	}

	outPath := filepath.Join(downloadsDir, sanitizedFilename)

	absDownloadsDir, err := filepath.Abs(downloadsDir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for downloads directory: %w", err)
	}

	absOutPath, err := filepath.Abs(outPath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for output file: %w", err)
	}

	rel, err := filepath.Rel(absDownloadsDir, absOutPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, "..\\") {
		return fmt.Errorf("invalid output path, directory traversal detected: %s", outPath)
	}

	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

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
	throughputMB := (float64(received) / (1024 * 1024)) / duration.Seconds()

	// 4. Verify Integrity
	var computedChecksum [32]byte
	copy(computedChecksum[:], hasher.Sum(nil))

	integrityStr := "VERIFIED"
	if !bytes.Equal(computedChecksum[:], header.Checksum[:]) {
		integrityStr = "FAILED"
		log.Printf("[WARNING] Checksum mismatch! Expected %x, got %x", header.Checksum, computedChecksum)
	}

	// Determine Connection Type
	connType := "Direct"
	if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
		connType = "Relay"
	}

	log.Printf("\nTransfer Complete (Receiver)")
	log.Printf("Path       : %s", connType)
	log.Printf("Integrity  : %s", integrityStr)
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}
