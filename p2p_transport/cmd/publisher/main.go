package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/discovery"
	"cipher/internal/distribution"
	"cipher/internal/identity"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"

	golog "github.com/ipfs/go-log/v2"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
)

func main() {
	golog.SetAllLoggers(golog.LevelWarn)

	filePath := flag.String("file", "", "Path to the file to ingest and publish (required)")
	port := flag.Int("p", 4005, "Port for the publisher to listen on (TCP)")
	wsPort := flag.Int("ws-port", 4006, "Port for the publisher to listen on (WebSocket, 0 to disable)")
	storePath := flag.String("store", "./store_publisher", "Path to local content store directory")
	bootstrapAddr := flag.String("bootstrap", "", "Bootstrap peer multiaddress")
	relayAddr := flag.String("relay", "", "Static relay multiaddress to use for NAT traversal")
	forceRelay := flag.Bool("force-relay", false, "Force traffic over relay")
	seed := flag.Bool("seed", true, "Keep publisher running to seed chunks over /cipher/chunk/1.0.0")
	identityPath := flag.String("identity", "", "Custom path to identity key file (optional)")
	chunkSizeKB := flag.Int("chunk-size", 32, "Chunk size in KB (default: 32)")
	providersList := flag.String("providers", "", "Comma-separated multiaddresses of target providers to push content to")
	replication := flag.Int("replication", 2, "Replication factor R (replicas per chunk across providers)")
	push := flag.Bool("push", false, "Push chunks to remote providers over /cipher/push/1.0.0 and exit")
	pushTimeout := flag.Duration("push-timeout", 5*time.Minute, "Timeout for remote push distribution")
	showKey := flag.Bool("show-key", false, "Display full unmasked decryption key")

	flag.Parse()

	if *filePath == "" {
		log.Fatalf("Error: -file <path> is required to publish content")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize identity
	var priv libp2pcrypto.PrivKey
	var err error
	if *identityPath != "" {
		priv, err = identity.LoadOrCreateFromPath(*identityPath)
	} else {
		priv, err = identity.LoadOrCreate()
	}
	if err != nil {
		log.Fatalf("Failed to load or create identity: %v", err)
	}

	// 2. Start libp2p host and DHT
	h, kdht, err := transport.NewNode(ctx, *port, *wsPort, priv, *relayAddr, *forceRelay)
	if err != nil {
		log.Fatalf("Failed to create libp2p node: %v", err)
	}
	defer h.Close()
	defer kdht.Close()

	// 3. Connect to DHT bootstrap if provided
	if *bootstrapAddr != "" {
		bootstrapInfo, err := peer.AddrInfoFromString(*bootstrapAddr)
		if err != nil {
			log.Fatalf("Invalid bootstrap address: %v", err)
		}
		if err := discovery.Bootstrap(ctx, kdht, h, []peer.AddrInfo{*bootstrapInfo}); err != nil {
			log.Fatalf("Failed to bootstrap DHT: %v", err)
		}
		log.Printf("[DHT] Bootstrap complete. Routing table has %d peers", len(kdht.RoutingTable().ListPeers()))
	}

	// Connect to relay if specified
	if *relayAddr != "" {
		relayInfo, err := peer.AddrInfoFromString(*relayAddr)
		if err == nil {
			if err := h.Connect(ctx, *relayInfo); err != nil {
				log.Printf("Warning: Failed to connect to relay: %v", err)
			} else {
				if res, err := client.Reserve(ctx, h, *relayInfo); err == nil {
					h.ConnManager().Protect(relayInfo.ID, "relay")
					log.Printf("[✓] Connected to relay and reserved slot (expires: %s)", res.Expiration.String())
				}
			}
		}
	}

	// 4. Initialize Content Engine
	if err := storage.NewFSStorage(*storePath); err != nil {
		log.Fatalf("Failed to create store dir: %v", err)
	}
	config := core.EngineConfig{ChunkSize: uint32((*chunkSizeKB) * 1024)}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(*storePath)
	eng := engine.NewContentEngine(config, enc, dig, store, store, keys, store)

	// Register chunk protocol stream handler for initial seeding
	chunk.NewStreamHandler(h, eng)

	// 5. Ingest Content
	log.Printf("Ingesting source file: %s", *filePath)
	f, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Failed to open file for ingest: %v", err)
	}
	defer f.Close()

	m, err := eng.Ingest(ctx, f, manifest.TypeFile)
	if err != nil {
		log.Fatalf("Failed to ingest file: %v", err)
	}

	// Persist manifest in engine
	mBytes, err := m.Serialize()
	if err != nil {
		log.Fatalf("Failed to serialize manifest: %v", err)
	}
	if err := eng.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		log.Fatalf("Failed to store manifest: %v", err)
	}

	// 6. Execute Remote Push if requested
	if *push {
		if *providersList == "" {
			log.Fatalf("Error: -push requires -providers <comma-separated provider multiaddresses>")
		}

		t := transport.NewTransport(h)
		var targetPeers []peer.ID

		for _, pAddrStr := range strings.Split(*providersList, ",") {
			pAddrStr = strings.TrimSpace(pAddrStr)
			if pAddrStr == "" {
				continue
			}
			addrInfo, err := t.Connect(ctx, pAddrStr)
			if err != nil {
				log.Printf("[Publisher] Warning: Failed to connect to provider %s: %v", pAddrStr, err)
				continue
			}
			targetPeers = append(targetPeers, addrInfo.ID)
		}

		if len(targetPeers) == 0 {
			log.Fatalf("Fatal: Could not connect to any specified target providers")
		}

		effectiveReplication := *replication
		if effectiveReplication > len(targetPeers) {
			effectiveReplication = len(targetPeers)
			log.Printf("[Publisher] Notice: Reduced replication to %d to match available provider count", effectiveReplication)
		}

		plan, err := distribution.PlanPlacement(m, targetPeers, effectiveReplication)
		if err != nil {
			log.Fatalf("Failed to plan chunk placement: %v", err)
		}

		tracker := distribution.NewGlobalReplicaTracker(effectiveReplication)
		pushCtx, pushCancel := context.WithTimeout(ctx, *pushTimeout)
		defer pushCancel()

		if err := distribution.Distribute(pushCtx, t, eng, plan, tracker, distribution.DefaultUploaderConfig); err != nil {
			log.Fatalf("Push distribution failed to satisfy replication invariant: %v", err)
		}

		log.Printf("[Publisher] [✓] All chunks successfully committed with >= %d replicas across %d remote providers!",
			effectiveReplication, len(targetPeers))
	} else {
		// Traditional direct publisher DHT announcement
		log.Printf("[DHT] Announcing ContentID %x on DHT...", m.Descriptor.ID)
		if err := discovery.Provide(ctx, kdht, m.Descriptor.ID); err != nil {
			log.Printf("[DHT] Warning: Could not advertise on DHT: %v (ensure bootstrap node is active)", err)
		} else {
			log.Printf("[DHT] Successfully announced ContentID on DHT")
		}
	}

	key, _ := keys.Get(ctx, m.Descriptor.ID)

	fmt.Println("\n================ CIPHER PUBLISHER ================")
	fmt.Printf("File Ingested : %s\n", *filePath)
	fmt.Printf("ContentID     : %x\n", m.Descriptor.ID)
	fmt.Printf("Decryption Key: %s\n", engine.FormatMaskedKey(key, *showKey))
	fmt.Printf("Chunks Total  : %d (%d KB per chunk)\n", len(m.ChunkIDs), *chunkSizeKB)
	fmt.Printf("Publisher ID  : %s\n", h.ID().String())
	fmt.Println("Addresses:")
	for _, addr := range h.Addrs() {
		fmt.Printf("  - %s/p2p/%s\n", addr.String(), h.ID().String())
	}
	fmt.Println("===================================================")

	if !*seed {
		log.Println("Seeding flag is false, exiting publisher.")
		return
	}

	log.Println("\n[Publisher] Seeding content over /cipher/chunk/1.0.0. Press Ctrl+C to stop.")

	// Wait for OS shutdown signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Println("Shutting down publisher...")
}
