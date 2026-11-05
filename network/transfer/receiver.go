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

// SanitizeAndValidatePath sanitizes the wire header filename and validates that the resolved output path
// stays strictly within the canonical path of the designated downloads directory.
func SanitizeAndValidatePath(downloadsDir, rawFilename string) (string, error) {
	// 1. Convert backslashes to forward slashes for cross-platform consistency
	normalized := strings.ReplaceAll(rawFilename, "\\", "/")

	// 2. Extract single base filename component
	cleaned := filepath.Clean(normalized)
	base := filepath.Base(cleaned)

	// 3. Assign safe fallback filename if sanitization yields an empty, dot, or separator string
	if base == "" || base == "." || base == ".." || base == "/" || base == "\\" || base == string(filepath.Separator) || strings.TrimSpace(base) == "" {
		base = "downloaded_file"
	}

	// 4. Resolve absolute path of downloads directory
	absDownloads, err := filepath.Abs(downloadsDir)
	if err != nil {
		log.Printf("[ERROR] Failed to get absolute path for downloads directory %s: %v", downloadsDir, err)
		return "", fmt.Errorf("failed to resolve downloads directory: %w", err)
	}
	absDownloads = filepath.Clean(absDownloads)

	// 5. Construct target path and resolve absolute target path
	targetPath := filepath.Join(downloadsDir, base)
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		log.Printf("[ERROR] Failed to resolve target output path %s: %v", targetPath, err)
		return "", fmt.Errorf("failed to resolve target path: %w", err)
	}
	absTarget = filepath.Clean(absTarget)

	// 6. Confirm directory containment using filepath.Rel
	rel, err := filepath.Rel(absDownloads, absTarget)
	if err != nil {
		log.Printf("[ERROR] Path containment check failed for %s against downloads directory %s: %v", absTarget, absDownloads, err)
		return "", fmt.Errorf("path containment check failed: %w", err)
	}

	relClean := filepath.Clean(rel)
	if relClean == "." || relClean == ".." || strings.HasPrefix(relClean, ".."+string(filepath.Separator)) || strings.HasPrefix(relClean, "../") || strings.HasPrefix(relClean, "..\\") || filepath.IsAbs(relClean) {
		log.Printf("[ERROR] Path containment violation: filename %q resolved outside downloads directory (%s)", rawFilename, absTarget)
		return "", fmt.Errorf("path containment violation: output file %q is outside downloads directory", absTarget)
	}

	return targetPath, nil
}

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

	outPath, err := SanitizeAndValidatePath(downloadsDir, header.Filename)
	if err != nil {
		log.Printf("[ERROR] Failed to validate output path for filename %q: %v", header.Filename, err)
		return fmt.Errorf("failed to validate output path: %w", err)
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
