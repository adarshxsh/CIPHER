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

	// Construct and Write Header
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

	// Send Data with Progress Tracking and In-Stream Checksum Calculation
	log.Printf("Sending: %s (%.2f MB)", header.Filename, float64(header.FileSize)/(1024*1024))

	startTime := time.Now()

	fileHasher := sha256.New()
	teeReader := io.TeeReader(file, fileHasher)

	pr := &progressReader{
		r:     teeReader,
		total: header.FileSize,
		last:  0,
	}

	buf := make([]byte, 32*1024)
	var written uint64
	var chunkIndex int

	for {
		n, readErr := pr.Read(buf)
		if n > 0 {
			chunkData := buf[:n]

			// Calculate per-chunk digest in-stream
			chunkDigest := sha256.Sum256(chunkData)
			_ = chunkDigest

			wn, writeErr := s.Write(chunkData)
			written += uint64(wn)
			if writeErr != nil {
				return fmt.Errorf("failed to send file data: %w", writeErr)
			}
			if wn < n {
				return fmt.Errorf("short write: wrote %d of %d bytes", wn, n)
			}
			chunkIndex++
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return fmt.Errorf("failed to read file data: %w", readErr)
		}
	}

	var calculatedChecksum [32]byte
	copy(calculatedChecksum[:], fileHasher.Sum(nil))

	duration := time.Since(startTime)
	throughputMB := (float64(written) / (1024 * 1024)) / duration.Seconds()

	// Determine Connection Type
	connType := "Direct"
	if _, err := s.Conn().RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
		connType = "Relay"
	}

	log.Printf("\nTransfer Complete (Sender)")
	log.Printf("Path       : %s", connType)
	log.Printf("SHA-256    : %x", calculatedChecksum)
	log.Printf("Chunks Sent: %d", chunkIndex)
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
