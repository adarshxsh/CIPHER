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

// SanitizeAndValidatePath normalizes rawFilename, extracts its single base filename,
// joins it with baseDir, and verifies that the resulting target path remains strictly
// within the canonical baseDir.
func SanitizeAndValidatePath(baseDir string, rawFilename string) (string, error) {
	// 1. Normalize Windows backslashes to POSIX forward slashes across platforms
	normalized := strings.ReplaceAll(rawFilename, "\\", "/")

	// 2. Clean path
	cleaned := filepath.Clean(normalized)

	// 3. Reject empty, dot, dot-dot, or separator-only filenames
	if rawFilename == "" || cleaned == "." || cleaned == ".." || cleaned == "/" || cleaned == "\\" {
		log.Printf("[SECURITY WARNING] Rejected invalid header filename: %q", rawFilename)
		return "", fmt.Errorf("invalid or empty filename: %q", rawFilename)
	}

	// 4. Extract base filename
	base := filepath.Base(cleaned)
	if base == "." || base == ".." || base == "/" || base == "\\" || base == "" || strings.ContainsAny(base, "/\\") {
		log.Printf("[SECURITY WARNING] Rejected filename with invalid base component: %q (extracted: %q)", rawFilename, base)
		return "", fmt.Errorf("invalid base filename extracted from %q: %q", rawFilename, base)
	}

	// 5. Compute canonical absolute path for baseDir
	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for download directory %q: %w", baseDir, err)
	}

	// 6. Construct target path
	outPath := filepath.Join(baseDir, base)
	absOutPath, err := filepath.Abs(outPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for target path %q: %w", outPath, err)
	}

	// 7. Verify path containment within absBaseDir
	rel, err := filepath.Rel(absBaseDir, absOutPath)
	if err != nil {
		log.Printf("[SECURITY WARNING] Path containment check failed for %q: %v", absOutPath, err)
		return "", fmt.Errorf("path containment check failed: %w", err)
	}

	cleanRel := filepath.Clean(rel)
	if cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) || strings.HasPrefix(cleanRel, "../") || strings.HasPrefix(cleanRel, "..\\") {
		log.Printf("[SECURITY WARNING] Target path %q escapes download directory %q (rel: %q)", absOutPath, absBaseDir, rel)
		return "", fmt.Errorf("target path escapes download directory: %s", outPath)
	}

	return outPath, nil
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
		return fmt.Errorf("invalid transfer path: %w", err)
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
