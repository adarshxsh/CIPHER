package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"cipher-p2p/internal/identity"
	"cipher-p2p/internal/transport"
)

const ProtocolID = "/cipher/filetransfer/1.0.0"

func main() {
	// Parse command-line flags
	listenPort := flag.Int("listen", 0, "Port to listen on (0 for random)")
	targetAddr := flag.String("target", "", "Target peer multiaddress to dial (optional)")
	keyPath := flag.String("key", "identity.key", "Path to the identity key file")
	relayAddrStr := flag.String("relay", "", "Multiaddress of a relay to use (optional)")
	flag.Parse()

	// Configure slog
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Parse Relay Addr if provided
	var relayAddrs []peer.AddrInfo
	if *relayAddrStr != "" {
		maddr, err := multiaddr.NewMultiaddr(*relayAddrStr)
		if err != nil {
			slog.Error("Invalid relay multiaddress", "error", err)
			os.Exit(1)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			slog.Error("Failed to parse relay peer info", "error", err)
			os.Exit(1)
		}
		relayAddrs = append(relayAddrs, *info)
	}

	// 1. Load or Generate Identity Key
	slog.Info("Loading identity key", "path", *keyPath)
	privKey, err := identity.LoadOrGenerateKey(*keyPath)
	if err != nil {
		slog.Error("Failed to load or generate key", "error", err)
		os.Exit(1)
	}

	// 2. Start libp2p Host
	slog.Info("Starting libp2p host", "port", *listenPort)
	host, err := transport.NewHost(privKey, *listenPort, relayAddrs)
	if err != nil {
		slog.Error("Failed to create libp2p host", "error", err)
		os.Exit(1)
	}
	defer host.Close()

	slog.Info("Host started successfully", "peer_id", host.ID().String())
	for _, addr := range host.Addrs() {
		slog.Info("Listening on address", "addr", fmt.Sprintf("%s/p2p/%s", addr, host.ID()))
	}

	// 3. Set up Stream Handler for incoming connections
	host.SetStreamHandler(ProtocolID, func(s network.Stream) {
		slog.Info("New incoming stream", "peer", s.Conn().RemotePeer().String())
		
		// Read message from stream
		reader := bufio.NewReader(s)
		msg, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Failed to read from stream", "error", err)
			s.Reset()
			return
		}
		
		slog.Info("Received message", "message", msg, "from", s.Conn().RemotePeer().String())
		
		// Send a reply
		reply := fmt.Sprintf("Hello from %s!\n", host.ID().String())
		_, err = s.Write([]byte(reply))
		if err != nil {
			slog.Error("Failed to write to stream", "error", err)
			s.Reset()
			return
		}
		s.Close()
	})

	// 4. Dial target if provided
	if *targetAddr != "" {
		slog.Info("Dialing target", "target", *targetAddr)
		
		maddr, err := multiaddr.NewMultiaddr(*targetAddr)
		if err != nil {
			slog.Error("Invalid target multiaddress", "error", err)
			os.Exit(1)
		}

		
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			slog.Error("Failed to parse peer info from multiaddress", "error", err)
			os.Exit(1)
		}

		// Connect to the peer
		if err := host.Connect(context.Background(), *info); err != nil {
			slog.Error("Failed to connect to peer", "error", err)
			os.Exit(1)
		}
		slog.Info("Connected to peer", "peer_id", info.ID.String())

		// Phase 3: Give Hole Punching (DCUtR) 3 seconds to upgrade the connection from Relayed to Direct!
		// Opening streams during the exact moment a connection is being migrated can cause timeouts.
		slog.Info("Waiting 3 seconds for Hole Punching to establish a direct connection...")
		time.Sleep(3 * time.Second)

		// Open a stream
		stream, err := host.NewStream(context.Background(), info.ID, ProtocolID)
		if err != nil {
			slog.Error("Failed to open stream", "error", err)
			os.Exit(1)
		}

		// Send a message
		helloMsg := fmt.Sprintf("Hello from %s!\n", host.ID().String())
		_, err = stream.Write([]byte(helloMsg))
		if err != nil {
			slog.Error("Failed to write to stream", "error", err)
			stream.Reset()
			os.Exit(1)
		}

		// Read reply
		reader := bufio.NewReader(stream)
		reply, err := reader.ReadString('\n')
		if err != nil {
			slog.Error("Failed to read reply", "error", err)
			stream.Reset()
		} else {
			slog.Info("Received reply", "message", reply, "from", info.ID.String())
			stream.Close()
		}
	}

	// 5. Wait for termination signal
	slog.Info("Peer is running. Press Ctrl+C to stop.")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	// Phase 3: Monitor connections periodically to observe direct connection upgrades
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sigCh:
				return
			case <-ticker.C:
				conns := host.Network().Conns()
				if len(conns) > 0 {
					for _, c := range conns {
						maddr := c.RemoteMultiaddr().String()
						connType := "Direct"
						if strings.Contains(maddr, "p2p-circuit") {
							connType = "Relayed"
						}
						slog.Info("Active Connection", "peer", c.RemotePeer().String(), "type", connType, "addr", maddr)
					}
				}
			}
		}
	}()

	<-sigCh
	
	slog.Info("Shutting down...")
}
