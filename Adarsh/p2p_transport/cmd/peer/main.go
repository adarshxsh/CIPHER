package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/identity"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"

	"encoding/hex"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	golog "github.com/ipfs/go-log/v2"
)

func main() {
	// Enable libp2p debug logging for circuit v2 and identify
	golog.SetLogLevel("relay", "debug")
	golog.SetLogLevel("autorelay", "debug")
	golog.SetLogLevel("p2p-circuit", "debug")
	golog.SetLogLevel("identify", "debug")
	target := flag.String("d", "", "Target peer multiaddress to dial (e.g. /ip4/127.0.0.1/tcp/55555/p2p/Qm...)")
	port := flag.Int("p", 0, "Port to listen on (default 0 for random)")
	relayAddr := flag.String("relay", "", "Static relay multiaddress to use for NAT traversal")
	forceRelay := flag.Bool("force-relay", false, "Disable hole punching and force traffic over the relay")
	
	// Milestone 8 flags
	ingestFile := flag.String("ingest", "", "Path to file to ingest locally")
	fetchID := flag.String("fetch", "", "ContentID to fetch from target peer")
	reassembleOut := flag.String("reassemble", "", "Output path to reassemble the fetched ContentID")
	keyHex := flag.String("key", "", "Decryption key (hex) for reassembly")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	priv, err := identity.LoadOrCreate()
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	h, err := transport.NewNode(ctx, *port, priv, *relayAddr, *forceRelay)
	if err != nil {
		log.Fatalf("Failed to create libp2p node: %v", err)
	}

	// Setup Content Engine
	storeDir := "./content_store"
	if err := storage.NewFSStorage(storeDir); err != nil {
		log.Fatalf("Failed to create store dir: %v", err)
	}
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewXChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(storeDir)
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys)

	// Setup protocol handler
	chunk.NewStreamHandler(h, eng)

	log.Printf("Peer initialized with ID: %s", h.ID().String())
	log.Println("Listening on the following local addresses:")
	for _, addr := range h.Addrs() {
		log.Printf("  - %s/p2p/%s", addr.String(), h.ID().String())
	}

	if *relayAddr != "" {
		relayInfo, err := peer.AddrInfoFromString(*relayAddr)
		if err == nil {
			// Proactively connect and explicitly reserve a slot on the relay
			if err := h.Connect(ctx, *relayInfo); err != nil {
				log.Printf("Warning: Failed to connect to relay: %v", err)
			} else {
				if res, err := client.Reserve(ctx, h, *relayInfo); err != nil {
					log.Printf("Warning: Failed to reserve slot on relay: %v", err)
				} else {
					log.Printf("\n[✓] Successfully connected to relay and reserved slot!")
					log.Printf("    Reservation Expiration: %s", res.Expiration.String())
					log.Printf("    Relay Peer ID: %s", relayInfo.ID.String())
					log.Printf("Your Relayed Multiaddress (Share this with peers to connect to you):")
					log.Printf("  - %s/p2p-circuit/p2p/%s\n", *relayAddr, h.ID().String())
				}
			}
		}
	}

	if *ingestFile != "" {
		log.Printf("Ingesting file: %s", *ingestFile)
		f, err := os.Open(*ingestFile)
		if err != nil {
			log.Fatalf("Failed to open ingest file: %v", err)
		}
		defer f.Close()

		m, err := eng.Ingest(ctx, f, manifest.TypeFile)
		if err != nil {
			log.Fatalf("Failed to ingest: %v", err)
		}
		// Save manifest bytes to engine memory so it can be served
		mBytes, _ := m.Serialize()
		eng.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
		
		key, _ := keys.Get(ctx, m.Descriptor.ID)
		log.Printf("[✓] Ingest complete!")
		log.Printf("    ContentID: %x", m.Descriptor.ID)
		log.Printf("    Key: %x", key)
	}

	if *target != "" && *fetchID != "" {
		log.Printf("Dialing target peer: %s", *target)
		t := transport.NewTransport(h)
		
		addrInfo, err := t.Connect(ctx, *target)
		if err != nil {
			log.Fatalf("Failed to connect to target: %v", err)
		}

		cIDBytes, err := hex.DecodeString(*fetchID)
		if err != nil || len(cIDBytes) != 32 {
			log.Fatalf("Invalid ContentID hex")
		}
		var contentID core.ContentID
		copy(contentID[:], cIDBytes)

		if *keyHex != "" {
			kBytes, err := hex.DecodeString(*keyHex)
			if err != nil || len(kBytes) != 32 {
				log.Fatalf("Invalid key hex format or length (must be 32 bytes)")
			}
			keys.Put(ctx, contentID, kBytes)
		}

		client, err := chunk.NewClient(ctx, h, addrInfo.ID, eng)
		if err != nil {
			log.Fatalf("Failed to create chunk client: %v", err)
		}
		defer client.Close()

		log.Printf("Resolving manifest for ContentID: %x", contentID)
		mData, err := client.Resolve(ctx, contentID)
		if err != nil {
			log.Fatalf("Failed to resolve manifest: %v", err)
		}

		m, err := manifest.Deserialize(mData)
		if err != nil {
			log.Fatalf("Failed to deserialize manifest: %v", err)
		}

		log.Printf("Downloading %d chunks...", len(m.ChunkIDs))
		if err := client.Download(ctx, m.ChunkIDs); err != nil {
			log.Fatalf("Download failed: %v", err)
		}

		log.Printf("[✓] Download complete!")

		if *reassembleOut != "" {
			outF, err := os.Create(*reassembleOut)
			if err != nil {
				log.Fatalf("Failed to create out file: %v", err)
			}
			defer outF.Close()
			if err := eng.Reassemble(ctx, m, outF); err != nil {
				log.Fatalf("Reassemble failed: %v", err)
			}
			log.Printf("[✓] Reassembled to: %s", *reassembleOut)
		}
	}

	// Wait for termination signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	fmt.Println()
	log.Println("Shutting down peer...")
	if err := h.Close(); err != nil {
		log.Fatalf("Failed to close host: %v", err)
	}
}
