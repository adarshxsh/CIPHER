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

// Send transfers a file to the remote peer over the provided stream.
func Send(s network.Stream, filePath string) error {
	if s == nil {
		return ErrNilStream
	}
	if filePath == "" {
		s.Reset()
		return ErrEmptyFilePath
	}

	filename := filepath.Base(filePath)
	if len(filename) > MaxFilenameSize {
		s.Reset()
		return ErrFilenameLengthInvalid
	}

	file, err := os.Open(filePath)
	if err != nil {
		s.Reset()
		return fmt.Errorf("failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		s.Reset()
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// 1. Construct and Write Header Metadata
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(info.Size()),
	}

	if err := header.WriteTo(s); err != nil {
		file.Close()
		s.Reset()
		return fmt.Errorf("failed to write header: %w", err)
	}

	// 2. Stream Data with Progress Tracking and Inline Checksum Calculation (Single Pass)
	log.Printf("Sending: %s (%.2f MB)", header.Filename, float64(header.FileSize)/(1024*1024))

	startTime := time.Now()
	hasher := sha256.New()
	multiWriter := io.MultiWriter(s, hasher)

	pr := &progressReader{
		r:     file,
		total: header.FileSize,
		last:  0,
	}

	written, err := io.Copy(multiWriter, pr)
	file.Close()

	if err != nil {
		s.Reset()
		return fmt.Errorf("failed to send file data: %w", err)
	}

	if uint64(written) != header.FileSize {
		s.Reset()
		return fmt.Errorf("sent size mismatch: expected %d, got %d", header.FileSize, written)
	}

	// 3. Send Calculated Checksum Trailer
	copy(header.Checksum[:], hasher.Sum(nil))
	if err := header.WriteChecksum(s); err != nil {
		s.Reset()
		return fmt.Errorf("failed to write checksum trailer: %w", err)
	}

	s.Close()

	duration := time.Since(startTime)
	throughputMB := (float64(written) / (1024 * 1024)) / duration.Seconds()

	// Determine Connection Type
	connType := "Direct"
	if s.Conn() != nil && s.Conn().RemoteMultiaddr() != nil {
		if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			connType = "Relay"
		}
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
