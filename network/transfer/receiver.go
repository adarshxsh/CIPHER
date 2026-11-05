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
func Receive(s network.Stream) (err error) {
	defer func() {
		if err != nil {
			_ = s.Reset()
		} else {
			if closeErr := s.Close(); closeErr != nil && err == nil {
				err = fmt.Errorf("failed to close stream: %w", closeErr)
			}
		}
	}()

	log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())

	// 1. Read Header
	var header Header
	if err = header.ReadFrom(s); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		return fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
	}

	if strings.Contains(header.Filename, "..") || filepath.IsAbs(header.Filename) || strings.HasPrefix(header.Filename, "/") || strings.HasPrefix(header.Filename, "\\") {
		return fmt.Errorf("path traversal detected in filename: %q", header.Filename)
	}

	cleanFilename := filepath.Base(filepath.ToSlash(header.Filename))
	if cleanFilename == "." || cleanFilename == ".." || cleanFilename == "" {
		return fmt.Errorf("invalid or unsafe filename in header: %q", header.Filename)
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err = os.MkdirAll(downloadsDir, 0755); err != nil {
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	downloadsAbs, errAbs := filepath.Abs(downloadsDir)
	if errAbs != nil {
		return fmt.Errorf("failed to get absolute downloads path: %w", errAbs)
	}

	outPath := filepath.Join(downloadsDir, cleanFilename)
	outPathAbs, errAbs := filepath.Abs(outPath)
	if errAbs != nil {
		return fmt.Errorf("failed to get absolute output path: %w", errAbs)
	}

	rel, errRel := filepath.Rel(downloadsAbs, outPathAbs)
	if errRel != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("path traversal detected in filename: %q", header.Filename)
	}

	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	tmpPath := outPath + ".tmp"
	outFile, createErr := os.Create(tmpPath)
	if createErr != nil {
		return fmt.Errorf("failed to create output file: %w", createErr)
	}

	var fileClosed bool
	defer func() {
		if !fileClosed {
			if closeErr := outFile.Close(); closeErr != nil && err == nil {
				err = fmt.Errorf("failed to close output file: %w", closeErr)
			}
		}
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	startTime := time.Now()

	// 3. Receive Data with Progress Tracking and Hashing
	hasher := sha256.New()
	multiWriter := io.MultiWriter(outFile, hasher)

	pr := &progressReader{
		r:     io.LimitReader(s, int64(header.FileSize)),
		total: header.FileSize,
		last:  0,
	}

	received, copyErr := io.Copy(multiWriter, pr)
	if copyErr != nil {
		return fmt.Errorf("failed to receive file data: %w", copyErr)
	}

	if uint64(received) != header.FileSize {
		return fmt.Errorf("received size mismatch: expected %d, got %d", header.FileSize, received)
	}

	duration := time.Since(startTime)
	throughputMB := (float64(received) / (1024 * 1024)) / duration.Seconds()

	// 4. Verify Integrity
	var computedChecksum [32]byte
	copy(computedChecksum[:], hasher.Sum(nil))

	if !bytes.Equal(computedChecksum[:], header.Checksum[:]) {
		log.Printf("[WARNING] Checksum mismatch! Expected %x, got %x", header.Checksum, computedChecksum)
		return fmt.Errorf("checksum mismatch: expected %x, got %x", header.Checksum, computedChecksum)
	}

	// Close outFile explicitly before atomic rename
	if closeErr := outFile.Close(); closeErr != nil {
		fileClosed = true
		return fmt.Errorf("failed to close output file: %w", closeErr)
	}
	fileClosed = true

	if renameErr := os.Rename(tmpPath, outPath); renameErr != nil {
		return fmt.Errorf("failed to rename output file: %w", renameErr)
	}

	// Determine Connection Type
	connType := "Direct"
	if _, errConn := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); errConn == nil {
		connType = "Relay"
	}

	log.Printf("\nTransfer Complete (Receiver)")
	log.Printf("Path       : %s", connType)
	log.Printf("Integrity  : VERIFIED")
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}
