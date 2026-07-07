package transport

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"cipher/internal/protocol"
)

// SetupStreamHandler configures the host to handle incoming streams for the file transfer protocol.
func SetupStreamHandler(h host.Host) {
	h.SetStreamHandler(protocol.FileTransferProtocolID, handleStream)
}

func handleStream(s network.Stream) {
	log.Printf("Got a new stream from %s!", s.Conn().RemotePeer())

	// Create a buffer stream for non blocking read and write.
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

	go readData(rw)
}

func readData(rw *bufio.ReadWriter) {
	for {
		str, err := rw.ReadString('\n')
		if err != nil {
			log.Println("Error reading from buffer:", err)
			return
		}

		if str == "" {
			return
		}
		if str != "\n" {
			log.Printf("Received: %s", str)
			// Automatically send "hello back\n" when "hello\n" is received for verification
			if str == "hello\n" {
				log.Println("Sending hello back...")
				_, err := rw.WriteString("hello back\n")
				if err != nil {
					log.Println("Error writing to buffer:", err)
					return
				}
				err = rw.Flush()
				if err != nil {
					log.Println("Error flushing buffer:", err)
					return
				}
			}
		}
	}
}

// Transport wraps the libp2p host to provide a simpler abstraction for connection and stream management.
type Transport struct {
	host host.Host
}

// NewTransport creates a new Transport abstraction.
func NewTransport(h host.Host) *Transport {
	return &Transport{host: h}
}

// Connect dials the target peer and establishes the initial connection (likely a relayed connection).
func (t *Transport) Connect(ctx context.Context, target string) (*peer.AddrInfo, error) {
	maddr, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		return nil, fmt.Errorf("invalid multiaddress: %w", err)
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract peer info: %w", err)
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	if err := t.host.Connect(dialCtx, *addrInfo); err != nil {
		return nil, fmt.Errorf("h.Connect failed: %w", err)
	}

	return addrInfo, nil
}

// OpenStream opens a new application stream to the target peer. It uses the best available connection.
func (t *Transport) OpenStream(ctx context.Context, target *peer.AddrInfo) (network.Stream, error) {
	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()

	// WithAllowLimitedConn serves as a fallback. If a direct connection (from DCUtR) is available,
	// libp2p will prefer it. If not, the application stream can still flow over the limited relay connection.
	streamCtx = network.WithAllowLimitedConn(streamCtx, "file-transfer-relay")

	s, err := t.host.NewStream(streamCtx, target.ID, protocol.FileTransferProtocolID)
	if err != nil {
		return nil, fmt.Errorf("NewStream failed: %w", err)
	}

	return s, nil
}

// InitiateFileTransfer starts the application protocol over an established stream.
func (t *Transport) InitiateFileTransfer(s network.Stream) error {
	log.Printf("Connected to %s, sending hello...", s.Conn().RemotePeer())
	
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	
	if _, err := rw.WriteString("hello\n"); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	
	go readData(rw)
	return nil
}
