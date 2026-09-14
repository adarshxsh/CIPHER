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
	defer func() {
		if err != nil {
			s.Reset()
		} else {
			s.Close()
		}
	}()

	log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())

	// 1. Read Header
	var header Header
	if readErr := header.ReadFrom(s); readErr != nil {
		err = fmt.Errorf("failed to read header: %w", readErr)
		return err
	}

	if header.Version != ProtocolVersion1 || header.Type != MsgTypeFileTransfer {
		err = fmt.Errorf("unsupported protocol version (%d) or message type (%d)", header.Version, header.Type)
		return err
	}

	// 2. Setup Downloads Directory
	downloadsDir := "downloads"
	if mkdirErr := os.MkdirAll(downloadsDir, 0755); mkdirErr != nil {
		err = fmt.Errorf("failed to create downloads directory: %w", mkdirErr)
		return err
	}

	outPath := filepath.Join(downloadsDir, filepath.Base(header.Filename))
	tmpPath := outPath + ".tmp"
	log.Printf("Receiving: %s (%.2f MB) into staging file %s", header.Filename, float64(header.FileSize)/(1024*1024), tmpPath)

	outFile, createErr := os.Create(tmpPath)
	if createErr != nil {
		err = fmt.Errorf("failed to create output file: %w", createErr)
		return err
	}

	fileClosed := false
	defer func() {
		if !fileClosed {
			outFile.Close()
		}
		if err != nil {
			os.Remove(tmpPath)
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
		err = fmt.Errorf("failed to receive file data: %w", copyErr)
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

	// Close temporary staging file prior to atomic rename
	if closeErr := outFile.Close(); closeErr != nil {
		fileClosed = true
		err = fmt.Errorf("failed to close temporary file: %w", closeErr)
		return err
	}
	fileClosed = true

	// Atomically rename temporary staging file to target destination path
	if renameErr := os.Rename(tmpPath, outPath); renameErr != nil {
		err = fmt.Errorf("failed to rename temporary file to target path: %w", renameErr)
		return err
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
