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
	var success bool
	var tmpPath string

	defer func() {
		if !success {
			if s != nil {
				s.Reset()
			}
			if tmpPath != "" {
				os.Remove(tmpPath)
			}
		} else {
			if s != nil {
				s.Close()
			}
		}
	}()

	remotePeer := "unknown"
	if s != nil && s.Conn() != nil {
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

	// 2. Setup Downloads Directory & Sanitize Path
	downloadsDir := "downloads"
	if err = os.MkdirAll(downloadsDir, 0755); err != nil {
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	safeFilename := filepath.Base(filepath.Clean(header.Filename))
	if safeFilename == "." || safeFilename == "/" || safeFilename == "" {
		safeFilename = "downloaded_file"
	}

	outPath := filepath.Join(downloadsDir, safeFilename)
	tmpPath = outPath + ".tmp"

	log.Printf("Receiving: %s (%.2f MB) into staging file %s", header.Filename, float64(header.FileSize)/(1024*1024), tmpPath)

	outFile, createErr := os.Create(tmpPath)
	if createErr != nil {
		return fmt.Errorf("failed to create staging file: %w", createErr)
	}

	writeAndVerify := func() error {
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
			return fmt.Errorf("checksum mismatch: expected %x, got %x", header.Checksum, computedChecksum)
		}

		// Determine Connection Type
		connType := "Direct"
		if s != nil && s.Conn() != nil && s.Conn().RemoteMultiaddr() != nil {
			if _, connErr := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); connErr == nil {
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

	if err = writeAndVerify(); err != nil {
		return err
	}

	// 5. Promote staging file to target output path
	if err = os.Rename(tmpPath, outPath); err != nil {
		return fmt.Errorf("failed to rename staging file: %w", err)
	}

	success = true
	return nil
}
