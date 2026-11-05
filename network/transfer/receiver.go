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
	var tmpPath string

	defer func() {
		if s != nil {
			if err != nil {
				_ = s.Reset()
			} else {
				if closeErr := s.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}
		}
		if err != nil && tmpPath != "" {
			_ = os.Remove(tmpPath)
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
		err = fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
		return err
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if err = os.MkdirAll(downloadsDir, 0755); err != nil {
		return fmt.Errorf("failed to create downloads directory: %w", err)
	}

	filename := filepath.Base(filepath.Clean(header.Filename))
	if filename == "" || filename == "." || filename == "/" || filename == ".." {
		filename = "downloaded_file"
	}

	finalPath := filepath.Join(downloadsDir, filename)
	tmpPath = filepath.Join(downloadsDir, "."+filename+".tmp")

	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), tmpPath)

	outFile, createErr := os.Create(tmpPath)
	if createErr != nil {
		err = fmt.Errorf("failed to create temporary file: %w", createErr)
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

	received, copyErr := io.Copy(multiWriter, pr)
	closeErr := outFile.Close()
	if copyErr != nil {
		err = fmt.Errorf("failed to receive file data: %w", copyErr)
		return err
	}
	if closeErr != nil {
		err = fmt.Errorf("failed to close temporary file: %w", closeErr)
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
		log.Printf("[WARNING] Checksum mismatch! Expected %x, got %x", header.Checksum, computedChecksum)
		err = fmt.Errorf("checksum mismatch: expected %x, got %x", header.Checksum, computedChecksum)
		return err
	}

	// Atomic promotion from temporary file to final destination
	if err = os.Rename(tmpPath, finalPath); err != nil {
		err = fmt.Errorf("failed to promote temporary file: %w", err)
		return err
	}

	// Determine Connection Type
	connType := "Direct"
	if s != nil && s.Conn() != nil && s.Conn().RemoteMultiaddr() != nil {
		if _, errConn := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); errConn == nil {
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
