package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"time"

	"cipher/network/content/core"
	"cipher/network/discovery"
	"cipher/network/transport"
	"cipher/shared/logger"

	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	golog.SetAllLoggers(golog.LevelWarn)

	log := logger.New("DHT-INSPECTOR")

	bootstrapAddr := flag.String("bootstrap", "", "Bootstrap peer multiaddress (required)")
	findCID := flag.String("find-cid", "", "ContentID hex to query Kademlia DHT providers for")
	listProviders := flag.Bool("list-providers", true, "Discover all active storage providers registered in DHT")
	timeoutSec := flag.Int("timeout", 5, "Timeout in seconds for DHT query")

	flag.Parse()

	if *bootstrapAddr == "" {
		log.Fatalf("Error: -bootstrap <multiaddr> is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	// 1. Create temporary inspecting libp2p host
	h, kdht, err := transport.NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		log.Fatalf("Failed to create inspecting DHT node: %v", err)
	}
	defer h.Close()
	defer kdht.Close()

	// 2. Connect to Bootstrap Node
	bootstrapInfo, err := peer.AddrInfoFromString(*bootstrapAddr)
	if err != nil {
		log.Fatalf("Invalid bootstrap address: %v", err)
	}

	if err := discovery.Bootstrap(ctx, kdht, h, []peer.AddrInfo{*bootstrapInfo}); err != nil {
		log.Fatalf("Failed to bootstrap DHT: %v", err)
	}

	rtPeers := kdht.RoutingTable().ListPeers()

	fields := []logger.Field{
		{Key: "Inspector Peer ID", Value: h.ID().String()},
		{Key: "Bootstrap Peer   ", Value: bootstrapInfo.ID.String()},
		{Key: "Routing Table Size", Value: fmt.Sprintf("%d connected peers", len(rtPeers))},
	}

	for idx, p := range rtPeers {
		fields = append(fields, logger.Field{
			Key:   fmt.Sprintf("  - Peer [%d]       ", idx+1),
			Value: p.String(),
		})
	}

	log.Banner("KADEMLIA DHT ROUTING TABLE INSPECTION", fields...)

	// 3. Query Storage Providers registered under DHT Namespace
	if *listProviders {
		log.Info("Querying Kademlia DHT for active storage provider registrations...")
		providers, err := discovery.FindStorageProviders(ctx, kdht, 16)
		if err != nil {
			log.Warn("Failed to query storage providers: %v", err)
		} else {
			log.Success("Discovered %d active storage provider(s) on Kademlia DHT:", len(providers))
			for i, p := range providers {
				fmt.Printf("   [%d] Provider Peer ID : %s\n", i+1, p.ID.String())
				for _, addr := range p.Addrs {
					fmt.Printf("       Multiaddress      : %s\n", addr.String())
				}
			}
		}
	}

	// 4. Query ContentID provider records if requested
	if *findCID != "" {
		cIDBytes, err := hex.DecodeString(*findCID)
		if err != nil || len(cIDBytes) != 32 {
			log.Fatalf("Invalid ContentID format (must be 32 bytes hex)")
		}
		var cid core.ContentID
		copy(cid[:], cIDBytes)

		log.Info("Querying Kademlia DHT for ContentID: %x...", cid)
		contentProviders, err := discovery.FindProviders(ctx, kdht, cid, 16)
		if err != nil {
			log.Warn("DHT query for ContentID %x failed: %v", cid, err)
		} else {
			log.Success("Kademlia DHT resolved %d provider(s) hosting ContentID %x:", len(contentProviders), cid)
			for i, p := range contentProviders {
				fmt.Printf("   [%d] Hosting Peer ID  : %s\n", i+1, p.ID.String())
				for _, addr := range p.Addrs {
					fmt.Printf("       Multiaddress      : %s\n", addr.String())
				}
			}
		}
	}

	fmt.Println()
}
