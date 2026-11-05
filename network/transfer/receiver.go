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

// SanitizeAndValidatePath sanitizes the filename using filepath.Base (handling cross-platform
// path separators) and checks that the cleaned destination path stays within downloadsDir.
func SanitizeAndValidatePath(filename string, downloadsDir string) (string, error) {
	// Standardize cross-platform path separators (convert backslashes to slashes)
	normalized := strings.ReplaceAll(filename, "\\", "/")

	// Extract base filename component
	base := filepath.Base(normalized)

	// Check for empty, root path, or special directory components
	if base == "." || base == ".." || base == "/" || base == "\\" || base == string(filepath.Separator) || strings.TrimSpace(base) == "" {
		log.Printf("[ERROR] Invalid header filename or path traversal attempt: %q", filename)
		return "", fmt.Errorf("invalid header filename: %q", filename)
	}

	cleanDownloads := filepath.Clean(downloadsDir)
	outPath := filepath.Join(cleanDownloads, base)
	cleanOut := filepath.Clean(outPath)

	// Ensure cleanOut has cleanDownloads as a directory prefix
	prefix := cleanDownloads
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}

	if !strings.HasPrefix(cleanOut, prefix) {
		log.Printf("[ERROR] Path traversal attempt detected: target path %q is outside download directory %q", cleanOut, cleanDownloads)
		return "", fmt.Errorf("target path %q is outside download directory %q", cleanOut, cleanDownloads)
	}

	return cleanOut, nil
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

	outPath, err := SanitizeAndValidatePath(header.Filename, downloadsDir)
	if err != nil {
		log.Printf("[ERROR] Transfer rejected due to invalid path/filename: %v", err)
		return fmt.Errorf("invalid target path: %w", err)
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
