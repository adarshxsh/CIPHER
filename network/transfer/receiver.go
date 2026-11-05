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

type deadlineReader struct {
	r       io.Reader
	s       network.Stream
	timeout time.Duration
}

func (dr *deadlineReader) Read(p []byte) (n int, err error) {
	if err := dr.s.SetReadDeadline(time.Now().Add(dr.timeout)); err != nil {
		return 0, err
	}
	return dr.r.Read(p)
}

// Receive accepts an incoming file transfer from the remote peer.
func Receive(s network.Stream) error {
	defer s.Close()

	peerID := s.Conn().RemotePeer()
	log.Printf("Incoming stream from %s. Preparing to receive...", peerID)

	// 1. Read Header with Deadline
	if err := s.SetReadDeadline(time.Now().Add(DefaultReadDeadline)); err != nil {
		return fmt.Errorf("failed to set header read deadline: %w", err)
	}

	var header Header
	if err := header.ReadFrom(s); err != nil {
		if isTimeout(err) {
			log.Printf("[Transfer] Read deadline expired while reading header from peer %s: %v", peerID, err)
		}
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

	outPath := filepath.Join(downloadsDir, header.Filename)
	log.Printf("Receiving: %s (%.2f MB) into %s", header.Filename, float64(header.FileSize)/(1024*1024), outPath)

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	startTime := time.Now()

	// 3. Receive Data with Progress Tracking, Sliding Read Deadlines and Hashing
	hasher := sha256.New()
	multiWriter := io.MultiWriter(outFile, hasher)

	dr := &deadlineReader{
		r:       io.LimitReader(s, int64(header.FileSize)),
		s:       s,
		timeout: DefaultReadDeadline,
	}

	pr := &progressReader{
		r:     dr,
		total: header.FileSize,
		last:  0,
	}

	received, err := io.Copy(multiWriter, pr)
	if err != nil && uint64(received) != header.FileSize {
		if isTimeout(err) {
			log.Printf("[Transfer] Read deadline expired while receiving file data from peer %s: %v", peerID, err)
		}
		return fmt.Errorf("failed to receive file data: %w", err)
	}

	if uint64(received) != header.FileSize {
		return fmt.Errorf("received size mismatch: expected %d, got %d", header.FileSize, received)
	}

	_ = s.SetDeadline(time.Time{})

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
	if remoteAddr := s.Conn().RemoteMultiaddr(); remoteAddr != nil {
		if _, err := remoteAddr.ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			connType = "Relay"
		}
	}

	log.Printf("\nTransfer Complete (Receiver)")
	log.Printf("Path       : %s", connType)
	log.Printf("Integrity  : %s", integrityStr)
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}
