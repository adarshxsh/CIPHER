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
// It computes the SHA-256 checksum in a single pass while streaming data using io.TeeReader,
// transmitting the header immediately and appending a 32-byte trailing checksum frame upon payload completion.
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

	// 1. Construct and Write Header immediately without pre-reading file data or seeking.
	// Checksum field is set to zero array [32]byte{} to signal trailing checksum frame mode.
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filepath.Base(filePath),
		FileSize: uint64(info.Size()),
		Checksum: [32]byte{},
	}

	if err := header.WriteTo(s); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// 2. Stream Data with Progress Tracking and SHA-256 computation in a single pass using io.TeeReader
	log.Printf("Sending: %s (%.2f MB)", header.Filename, float64(header.FileSize)/(1024*1024))

	startTime := time.Now()

	hasher := sha256.New()
	pr := &progressReader{
		r:     file,
		total: header.FileSize,
		last:  0,
	}

	// io.TeeReader writes bytes read from pr to hasher as they are read during io.Copy to s
	tee := io.TeeReader(pr, hasher)

	written, err := io.Copy(s, tee)
	if err != nil {
		return fmt.Errorf("failed to send file data: %w", err)
	}

	if uint64(written) != header.FileSize {
		return fmt.Errorf("sent size mismatch: expected %d, got %d", header.FileSize, written)
	}

	// 3. Write 32-byte trailing checksum frame immediately following final payload byte
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	if _, err := s.Write(checksum[:]); err != nil {
		return fmt.Errorf("failed to write trailing checksum frame: %w", err)
	}

	duration := time.Since(startTime)
	throughputMB := (float64(written) / (1024 * 1024)) / duration.Seconds()

	// Determine Connection Type
	connType := "Direct"
	if s.Conn() != nil {
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
