package transfer

import (
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

type deadlineWriter struct {
	s       network.Stream
	timeout time.Duration
}

func (dw *deadlineWriter) Write(p []byte) (int, error) {
	_ = dw.s.SetWriteDeadline(time.Now().Add(dw.timeout))
	n, err := dw.s.Write(p)
	if n > 0 {
		_ = dw.s.SetWriteDeadline(time.Now().Add(dw.timeout))
	}
	return n, err
}

// Send transfers a file to the remote peer over the provided stream.
func Send(s network.Stream, filePath string) error {
	defer s.Close()

	log.Printf("Preparing to send %s...", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// 1. Calculate Full SHA-256 Checksum
	log.Printf("Calculating SHA-256 for %s...", info.Name())
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return fmt.Errorf("failed to hash file: %w", err)
	}

	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	// Rewind file for sending
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to rewind file: %w", err)
	}

	// 2. Construct and Write Header
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filepath.Base(filePath),
		FileSize: uint64(info.Size()),
		Checksum: checksum,
	}

	if err := s.SetWriteDeadline(time.Now().Add(DefaultTransferDeadline)); err != nil {
		return fmt.Errorf("failed to set write deadline for header: %w", err)
	}

	if err := header.WriteTo(s); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	_ = s.SetWriteDeadline(time.Time{})

	// 3. Send Data with Progress Tracking
	log.Printf("Sending: %s (%.2f MB)", header.Filename, float64(header.FileSize)/(1024*1024))

	startTime := time.Now()

	// Create a progress reader
	pr := &progressReader{
		r:     file,
		total: header.FileSize,
		last:  0,
	}

	dw := &deadlineWriter{
		s:       s,
		timeout: DefaultTransferDeadline,
	}

	written, err := io.Copy(dw, pr)
	_ = s.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("failed to send file data: %w", err)
	}

	duration := time.Since(startTime)
	throughputMB := (float64(written) / (1024 * 1024)) / duration.Seconds()

	// Determine Connection Type
	connType := "Direct"
	if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
		connType = "Relay"
	}

	log.Printf("\nTransfer Complete (Sender)")
	log.Printf("Path       : %s", connType)
	log.Printf("Duration   : %s", duration.Round(time.Millisecond))
	log.Printf("Throughput : %.2f MB/s", throughputMB)

	return nil
}

type progressReader struct {
	r     io.Reader
	total uint64
	read  uint64
	last  int
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	pr.read += uint64(n)

	if pr.total > 0 {
		percent := int((float64(pr.read) / float64(pr.total)) * 100)
		if percent > pr.last && percent%10 == 0 {
			log.Printf("Progress: %d%%", percent)
			pr.last = percent
		}
	}

	return n, err
}
