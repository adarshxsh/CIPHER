# CIPHER P2P Transport

This repository contains the peer-to-peer transport layer for the CIPHER protocol. It provides NAT-traversal, relay-based connectivity, and will eventually support the full CIPHER 4-message handshake for verified, encrypted file transfer.

## Phase Status

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 — Identity | ✅ Complete | Persistent Ed25519 keys, local loopback hello exchange |
| Phase 2 — Relay Bootstrap | ✅ Complete | Render-hosted relay (WSS), cross-network connectivity |
| Phase 3 — Hole Punching | ⚠️ Reverted | DCUtR implemented but causes stream race conditions. Deferred until transport is proven stable |
| Phase 4 — Chunk Engine | 🔲 Planned | 32KB chunking, AES-GCM encryption, keccak256 commitments, Merkle tree |
| Phase 5 — Wire Protocol | 🔲 Planned | ChunkRequest / ChunkResponse / LotteryTicket / KeyReveal over libp2p streams |

## Project Structure

```
p2p_transport/
├── cmd/
│   ├── peer/main.go        # CLI peer node (sender or receiver)
│   └── relay/main.go       # Relay node (deployed to Render)
├── internal/
│   ├── identity/            # Ed25519 key generation and persistence
│   ├── transport/           # libp2p host configuration
│   └── testing/             # Stress test harness
├── docs/                    # Architecture, testing, production docs
├── Makefile                 # Build, test, and run shortcuts
├── Dockerfile               # Container build for relay deployment
└── docker-compose.yml       # Local multi-node testing
```

## Quick Start

### Prerequisites
- Go 1.25+ installed
- (Optional) Docker for local multi-node testing

### macOS / Linux

```bash
# Terminal 1 — Start the relay locally
make run-relay

# Terminal 2 — Start Peer 1 (receiver)
make run-peer1 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>

# Terminal 3 — Start Peer 2 (sender, dials Peer 1 via relay circuit)
make run-peer2 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID> TARGET=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

### Windows (Standalone Binary)

No Go, Make, or Docker required. Just the pre-compiled `.exe`:

```powershell
.\cipher-peer.exe -listen 9001 -relay /dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<RELAY_ID> -target /dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

Cross-compile the Windows binary from macOS:
```bash
make build-windows
```

### Cloud Relay (Render)

The relay is deployed at `cipher-sk0s.onrender.com`. Connect using WebSockets:

```bash
make run-peer1 RELAY=/dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<RELAY_ID>
```

> **Note**: Render's free tier spins down on inactivity. The first connection after idle may take 30-60 seconds while it wakes up. The peer will retry automatically with exponential backoff.

## Documentation

- [Testing Guide](docs/TESTING.md) — How to test across platforms
- [Architecture](docs/Architecture.md) — Phase diagrams and transport design
- [Production Considerations](docs/PRODUCTION.md) — Deployment and security notes
- [Roadmap](docs/ROADMAP.md) — Full phase-by-phase roadmap
- [Phase 3 Debugging](docs/Phase3_Debugging.md) — Lessons learned from hole punching
- [Relay Analysis](docs/Relay_Analysis.md) — How the relay works under the hood
