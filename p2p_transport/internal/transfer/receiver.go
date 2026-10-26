package transfer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

const DefaultFilename = "downloaded_file"

// SanitizeFilename extracts the base filename component from wire transfer headers,
// handling cross-platform path separators ('/' and '\'), stripping control characters,
// and mapping root, current directory, or empty path identifiers to a safe default filename.
func SanitizeFilename(filename string) string {
	// Remove null bytes
	cleaned := strings.ReplaceAll(filename, "\x00", "")

	// Normalize Windows backslashes to forward slashes for uniform cross-platform handling
	normalized := strings.ReplaceAll(cleaned, "\\", "/")

	// Extract base component using path.Base (which handles forward slashes)
	base := path.Base(normalized)

	// Clean trimmed base
	base = strings.TrimSpace(base)

	// Fallback to default filename if base is root, current/parent directory, or empty
	if base == "" || base == "." || base == ".." || base == "/" || base == "\\" {
		return DefaultFilename
	}

	return base
}

// ValidateDestinationPath ensures that the target destination path resides strictly
// within the designated downloads directory boundary.
func ValidateDestinationPath(downloadsDir, filename string) (string, error) {
	safeFilename := SanitizeFilename(filename)

	absDownloads, err := filepath.Abs(filepath.Clean(downloadsDir))
	if err != nil {
		return "", fmt.Errorf("failed to resolve downloads directory path: %w", err)
	}

	if evalDir, err := filepath.EvalSymlinks(absDownloads); err == nil {
		absDownloads = evalDir
	}

	targetPath := filepath.Join(absDownloads, safeFilename)
	absTarget, err := filepath.Abs(filepath.Clean(targetPath))
	if err != nil {
		return "", fmt.Errorf("failed to resolve target path: %w", err)
	}

	rel, err := filepath.Rel(absDownloads, absTarget)
	if err != nil {
		return "", fmt.Errorf("path traversal detected: failed to compute relative path: %w", err)
	}

	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.HasPrefix(rel, "../") || strings.HasPrefix(rel, "..\\") {
		return "", fmt.Errorf("path traversal detected: target path %q escapes downloads directory %q", absTarget, absDownloads)
	}

	expectedPrefix := absDownloads
	if !strings.HasSuffix(expectedPrefix, string(filepath.Separator)) {
		expectedPrefix += string(filepath.Separator)
	}

	if !strings.HasPrefix(absTarget, expectedPrefix) {
		return "", fmt.Errorf("path containment check failed: target path %q is not within downloads directory %q", absTarget, absDownloads)
	}

	return absTarget, nil
}

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

	header.Filename = SanitizeFilename(header.Filename)
	outPath, err := ValidateDestinationPath(downloadsDir, header.Filename)
	if err != nil {
		return fmt.Errorf("invalid destination path: %w", err)
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
