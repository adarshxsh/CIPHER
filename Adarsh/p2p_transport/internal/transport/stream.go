package transport

import (
	"bufio"
	"context"
	"log"

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

// ConnectAndSayHello connects to a target peer and sends a "hello" message.
func ConnectAndSayHello(ctx context.Context, h host.Host, target string) error {
	maddr, err := multiaddr.NewMultiaddr(target)
	if err != nil {
		return err
	}
	
	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return err
	}
	
	if err := h.Connect(ctx, *addrInfo); err != nil {
		return err
	}
	
	s, err := h.NewStream(ctx, addrInfo.ID, protocol.FileTransferProtocolID)
	if err != nil {
		return err
	}
	
	log.Printf("Connected to %s, sending hello...", addrInfo.ID)
	
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	
	_, err = rw.WriteString("hello\n")
	if err != nil {
		return err
	}
	err = rw.Flush()
	if err != nil {
		return err
	}
	
	go readData(rw)
	
	return nil
}
