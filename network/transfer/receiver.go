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

var DefaultTransferDeadline = 30 * time.Second

type deadlineReader struct {
	s       network.Stream
	r       io.Reader
	timeout time.Duration
}

func (dr *deadlineReader) Read(p []byte) (int, error) {
	_ = dr.s.SetReadDeadline(time.Now().Add(dr.timeout))
	timer := time.AfterFunc(dr.timeout, func() {
		dr.s.Reset()
	})
	n, err := dr.r.Read(p)
	timer.Stop()
	if n > 0 {
		_ = dr.s.SetReadDeadline(time.Now().Add(dr.timeout))
	}
	return n, err
}

// Receive accepts an incoming file transfer from the remote peer.
func Receive(s network.Stream) error {
	defer s.Close()

	log.Printf("Incoming stream from %s. Preparing to receive...", s.Conn().RemotePeer())

	// 1. Read Header
	_ = s.SetReadDeadline(time.Now().Add(DefaultTransferDeadline))
	headerTimer := time.AfterFunc(DefaultTransferDeadline, func() {
		s.Reset()
	})

	var header Header
	err := header.ReadFrom(s)
	headerTimer.Stop()
	_ = s.SetReadDeadline(time.Time{})

	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	_ = s.SetReadDeadline(time.Time{})

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

	// 3. Receive Data with Progress Tracking and Hashing
	hasher := sha256.New()
	multiWriter := io.MultiWriter(outFile, hasher)

	dr := &deadlineReader{
		s:       s,
		r:       s,
		timeout: DefaultTransferDeadline,
	}

	pr := &progressReader{
		r:     io.LimitReader(dr, int64(header.FileSize)),
		total: header.FileSize,
		last:  0,
	}

	received, err := io.Copy(multiWriter, pr)
	_ = s.SetReadDeadline(time.Time{})
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
