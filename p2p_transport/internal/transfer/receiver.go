package transfer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

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

	outPath, err := SanitizeAndValidatePath(header.Filename, downloadsDir)
	if err != nil {
		return fmt.Errorf("invalid header filename or path containment failure: %w", err)
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

// SanitizeAndValidatePath sanitizes the wire header filename and verifies that
// the target output path resides strictly within the downloads directory.
func SanitizeAndValidatePath(filename string, downloadsDir string) (string, error) {
	if len(filename) == 0 {
		return "", errors.New("filename cannot be empty")
	}

	for _, r := range filename {
		if r == '\x00' || unicode.IsControl(r) {
			return "", fmt.Errorf("filename contains invalid character: %q", r)
		}
	}

	// Normalize backslashes to forward slashes
	normalized := strings.ReplaceAll(filename, "\\", "/")
	cleaned := filepath.Clean(normalized)

	// Strip volume name if present (e.g., "C:")
	vol := filepath.VolumeName(cleaned)
	pathNoVol := strings.TrimPrefix(cleaned, vol)

	base := filepath.Base(pathNoVol)
	if base == "." || base == ".." || base == "/" || base == "\\" || base == "" || strings.TrimSpace(base) == "" {
		return "", fmt.Errorf("invalid or traversal filename: %q", filename)
	}

	absDownloadsDir, err := filepath.Abs(downloadsDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for downloads directory: %w", err)
	}
	cleanDownloadsDir := filepath.Clean(absDownloadsDir)

	outPath := filepath.Join(cleanDownloadsDir, base)
	absOutPath, err := filepath.Abs(outPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for target file: %w", err)
	}
	cleanOutPath := filepath.Clean(absOutPath)

	rel, err := filepath.Rel(cleanDownloadsDir, cleanOutPath)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || strings.HasPrefix(rel, "../") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path traversal detected: %q is not contained within %q", cleanOutPath, cleanDownloadsDir)
	}

	expectedPrefix := cleanDownloadsDir + string(os.PathSeparator)
	if !strings.HasPrefix(cleanOutPath, expectedPrefix) {
		return "", fmt.Errorf("path containment failed: %q is outside %q", cleanOutPath, cleanDownloadsDir)
	}

	return cleanOutPath, nil
}
