# CIPHER Roadmap

## Phase 1: Foundation
- [x] Milestone 1: Repository Setup (Project structure, libp2p basic integration)
- [x] Milestone 2: Persistent Peer Identity (Ed25519 identity generation and persistence)
- [x] Milestone 3: Direct P2P File Transfer (Custom protocol `/cipher/filetransfer/1.0.0` over basic streams)

## Phase 2: Enhanced Connectivity
- [x] Milestone 4: Bare Relay Service (Basic `circuitv2` relay node creation)
- [x] Milestone 5: Relay Connectivity (Routing application streams over `circuitv2` limited connections)
- [x] Milestone 6: Hole Punching & DCUtR (Upgrading relay connections to direct TCP/UDP via hole punching)

## Phase 3: Protocol Refinement
- [x] Milestone 7: Content Engine Foundation (Chunking, Cryptography, Content-Addressed Storage, Manifests)
- [x] Milestone 8: Content-Addressed Protocol & Integration
- [x] Milestone 9: Reliable Content Transfer (Session Management, Resume, Retry)
- [x] Milestone 10: Multi-peer Swarming & Chunk Scheduling *(Successfully validated across multiple devices & NATs via public relays!)*

## Phase 4: Decentralization & Scaling
- [ ] Milestone 11: Discovery (mDNS & DHT routing)
- [ ] Milestone 12: Deduplication, Proof-of-Storage & CDN Cache Placement
- [ ] Milestone 13: End-to-End Testing & Polish


CGO_ENABLED=0 go run ./cmd/peer/main.go -p 55559 -ws-port 55560 -store ./store_b -relay /ip4/20.197.30.171/tcp/4001/p2p/12D3KooWG1iekoPPbk9HiiWzc4hxhN7L3ddNNYoJsRfaGGYs8ADo -bootstrap /ip4/20.197.30.171/tcp/4003/p2p/12D3KooWAeTavvFLTaP3ZkC6g6pWf7JfDXSFefWcrfGmVEDPJWTH -fetch f77c374da61241cb44543c004b8cd9c8fff9066043592eed9c79288af820f370 -key 823b7f9a3f0f46ad5c2e01115f8fb7e4116bbd6be6011fdd0db018d20ff63ebb -reassemble FATE.mp4
