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
	
	// Create a context with timeout for connecting
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()
	
	if err := h.Connect(dialCtx, *addrInfo); err != nil {
		return fmt.Errorf("h.Connect failed: %w", err)
	}
	
	log.Printf("[DIAGNOSTIC] Connect() succeeded. Connectedness: %s", h.Network().Connectedness(addrInfo.ID))
	
	// Enumerate existing connections to this peer
	conns := h.Network().ConnsToPeer(addrInfo.ID)
	log.Printf("[DIAGNOSTIC] Active connections to peer (%d):", len(conns))
	hasCircuit := false
	for i, conn := range conns {
		log.Printf("  Conn %d: Local: %s | Remote: %s", i, conn.LocalMultiaddr(), conn.RemoteMultiaddr())
		if _, err := conn.RemoteMultiaddr().ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			hasCircuit = true
		}
	}
	if !hasCircuit {
		log.Printf("[WARNING] No active /p2p-circuit connection found. Stream might drop or attempt direct dial.")
	} else {
		log.Printf("[DIAGNOSTIC] Relay circuit path verified.")
	}

	// Wait briefly to allow the Identify protocol to populate the PeerStore
	log.Printf("[DIAGNOSTIC] Waiting for Identify protocol to populate PeerStore...")
	time.Sleep(2 * time.Second)
	
	addrs := h.Peerstore().Addrs(addrInfo.ID)
	log.Printf("[DIAGNOSTIC] Known PeerStore addresses for %s:", addrInfo.ID)
	hasCircuitAddr := false
	for _, a := range addrs {
		log.Printf("  - %s", a.String())
		if _, err := a.ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			hasCircuitAddr = true
		}
	}
	if !hasCircuitAddr {
		log.Printf("[WARNING] Target peer does not have a /p2p-circuit address in PeerStore! Stream may fail.")
	}

	// Create a context with timeout for stream opening
	streamCtx, streamCancel := context.WithTimeout(ctx, 15*time.Second)
	defer streamCancel()
	
	s, err := h.NewStream(streamCtx, addrInfo.ID, protocol.FileTransferProtocolID)
	if err != nil {
		return fmt.Errorf("NewStream failed: %w", err)
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
