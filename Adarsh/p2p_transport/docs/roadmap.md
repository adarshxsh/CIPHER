# CIPHER Roadmap

## Phase 1: Foundation
- [x] Milestone 1: Repository Setup (Project structure, libp2p basic integration)
- [x] Milestone 2: Persistent Peer Identity (Ed25519 identity generation and persistence)
- [x] Milestone 3: Direct P2P File Transfer (Custom protocol `/cipher/filetransfer/1.0.0` over basic streams)

## Phase 2: Enhanced Connectivity
- [x] Milestone 4: Bare Relay Service (Basic `circuitv2` relay node creation)
- [x] Milestone 5: Relay Connectivity (Routing application streams over `circuitv2` limited connections)
- [x] Milestone 6: Hole Punching & DCUtR (Upgrading relay connections to direct TCP/UDP via hole punching)
- [ ] Milestone 7: Discovery (mDNS & DHT routing)

## Phase 3: Protocol Refinement
- [ ] Milestone 8: Chunking & Merkle Verification
- [ ] Milestone 9: Encryption & Authentication

## Phase 4: Release
- [ ] Milestone 10: End-to-End Testing & Polish
