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
	var outPath string
	var outFile *os.File

	defer func() {
		if outFile != nil {
			if closeErr := outFile.Close(); closeErr != nil {
				log.Printf("[WARNING] Failed to close output file %s: %v", outPath, closeErr)
			}
		}

		if err != nil {
			log.Printf("[ERROR] Transfer failed: %v. Resetting stream.", err)
			if resetErr := s.Reset(); resetErr != nil {
				log.Printf("[DEBUG] Failed to reset stream (may already be closed/reset): %v", resetErr)
			}

			if outPath != "" {
				if _, statErr := os.Stat(outPath); statErr == nil {
					log.Printf("[INFO] Removing incomplete or corrupted file: %s", outPath)
					if rmErr := os.Remove(outPath); rmErr != nil {
						log.Printf("[ERROR] Failed to remove partial file %s: %v", outPath, rmErr)
					}
				}
			}
		} else {
			if closeErr := s.Close(); closeErr != nil {
				log.Printf("[WARNING] Failed to close stream gracefully: %v", closeErr)
			}
		}
	}()

	log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())

	// 1. Read Header
	var header Header
	if err = header.ReadFrom(s); err != nil {
		err = fmt.Errorf("failed to read header: %w", err)
		return err
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		err = fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
		return err
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err = os.MkdirAll(downloadsDir, 0755); err != nil {
		err = fmt.Errorf("failed to create downloads directory: %w", err)
		return err
	}

	outPath = filepath.Join(downloadsDir, header.Filename)
	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	outFile, err = os.Create(outPath)
	if err != nil {
		err = fmt.Errorf("failed to create output file: %w", err)
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

	var received int64
	received, err = io.Copy(multiWriter, pr)
	if err != nil {
		err = fmt.Errorf("failed to receive file data: %w", err)
		return err
	}

	if uint64(received) != header.FileSize {
		err = fmt.Errorf("received size mismatch: expected %d, got %d", header.FileSize, received)
		return err
	}

	duration := time.Since(startTime)
	throughputMB := (float64(received) / (1024 * 1024)) / duration.Seconds()

	// 4. Verify Integrity
	var computedChecksum [32]byte
	copy(computedChecksum[:], hasher.Sum(nil))

	if !bytes.Equal(computedChecksum[:], header.Checksum[:]) {
		err = fmt.Errorf("checksum mismatch: expected %x, got %x", header.Checksum, computedChecksum)
		return err
	}

	// Determine Connection Type
	connType := "Direct"
	if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
		connType = "Relay"
	}

	log.Printf("\nTransfer Complete (Receiver)")
	log.Printf("Path       : %s", connType)
	log.Printf("Integrity  : VERIFIED")
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}
