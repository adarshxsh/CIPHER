# CIPHER P2P Roadmap

This document tracks the full development roadmap for the CIPHER P2P transport layer. It serves as the single source of truth for phase status and dependencies.

## Architecture Principle

> **Hole punching is an optimization, not a dependency.**

The CIPHER protocol receives a `network.Stream` (which implements `io.ReadWriter`). It does not know or care whether that stream runs over:
- A direct TCP connection (hole-punched)
- A relayed circuit (through Render)
- A WebSocket (through Render's load balancer)

This separation ensures protocol development is never blocked by transport instability.

```
Try Direct
      │
      ▼
  Success?
 ┌────┴────┐
Yes       No
 │         │
 ▼         ▼
Direct   Relay
 │         │
 └────┬────┘
      ▼
Run CIPHER Protocol
```

## Phase Overview

### ✅ Phase 1 — Identity & Local Loopback
**Commit**: `dc3c008`

- Ed25519 keypair generation and persistence
- libp2p host with custom protocol ID (`/cipher/filetransfer/1.0.0`)
- Hello message exchange between two local processes

### ✅ Phase 2 — Relay Bootstrap
**Commit**: `51f973b` → `b213020` (WebSocket support)

- Relay node with `libp2p.EnableRelayService()`
- Deployed to Render (WSS on port 443)
- Persistent identity via `CIPHER_IDENTITY_KEY` env var
- Cross-network connectivity: macOS ↔ Windows, different ISPs
- AutoRelay with static relay addresses

**Hardened in `phase2-stable`**:
- Connection manager (prune idle connections, maintain healthy peer count)
- Ping keepalive (detect dead connections)
- Timeout contexts for Connect/NewStream (never block indefinitely)
- Exponential backoff reconnection (handles Render cold starts)
- Relay resource limits (128 reservations, 64 circuits, 5min/128MB per circuit)
- HTTP `/health` endpoint for monitoring
- Stress test harness (transfer, long connection, connection state)

### ⚠️ Phase 3 — Hole Punching (DCUtR) — Deferred
**Commits**: `9a5e947` (implemented) → `bd36bda` (reverted)

**Known Bug**: `host.NewStream()` races with DCUtR connection migration and intermittently times out. The `time.Sleep(3s)` workaround is fragile.

**Return Conditions**:
1. Phase 2 stress tests pass consistently across all platforms
2. CIPHER protocol works reliably over relayed connections
3. Event-driven connection state machine replaces timing workarounds
4. Full diagnostic logging: `Reservation → Relay Connected → Hole Punch Started → Hole Punch Finished → Direct Connection`

### 🔲 Phase 4 — Chunk & Commitment Engine
Pure local work, no network involved.

- Split files into strict 32,768-byte (32 KB) chunks
- Per-chunk: generate random 32-byte key K, encrypt with AES-GCM
- Commitment: `H_resp = keccak256(K ‖ C_plaintext)`
- Merkle leaf: `keccak256(FileID ‖ ChunkIndex ‖ Length ‖ C_plaintext)`
- Build Merkle tree, output root as hex

**Verification**: CLI command prints a valid hexadecimal Merkle root from a local file.

### 🔲 Phase 5 — 4-Message Wire Protocol
The v5 packet sequence rides the libp2p stream.

```
Client                          Provider
  │                                │
  │── ChunkRequest(Index, Nonce) ──►
  │                                │
  │◄─ ChunkResponse(CT, H_resp) ──│
  │                                │
  │── LotteryTicket(Block, Sig) ──►
  │                                │
  │◄─── KeyReveal(Key_K) ─────────│
  │                                │
  │  Decrypt CT with K             │
  │  Verify H_resp == keccak(K‖PT) │
```

Packet encoding: `encoding/binary`, Big-Endian, 1-byte type + 4-byte length framing.

LotteryTicket.Sig uses the existing Ed25519 identity key (no second crypto primitive).

**Verification**: Full 4-message sequence transfers one 32 KB chunk intact across two peers on different networks.

### 🔲 Phase 6 — Demo Hardening
- Loop handshake per chunk to transfer whole files
- Explicit fallback message when hole punch fails
- Replace README Pinggy references with Peer ID + relay instructions

**Verification**: A full file transfers across two networks via the complete v5 packet flow with Merkle-rooted, commitment-verified integrity.

## Git Branching Strategy

```
main
 │
 ├── phase2-stable          ← Transport hardening (current)
 │
 ├── phase4-cipher          ← Chunk engine + wire protocol
 │
 └── phase3-holepunch       ← DCUtR with event-driven diagnostics
```

If DCUtR breaks, protocol development continues on top of the stable relay branch.

## Relay Deployment

| Property | Value |
|----------|-------|
| Provider | Render (Free Tier) |
| URL | `cipher-sk0s.onrender.com` |
| Transport | WebSocket only (WSS on port 443) |
| Identity | Persistent via `CIPHER_IDENTITY_KEY` env var |
| Max Reservations | 128 |
| Max Circuits | 64 |
| Circuit Lifetime | 5 minutes |
| Circuit Data Limit | 128 MB |
| Health Endpoint | `/health` on port 8080 |

> **Note**: This is a development/demonstration relay. For production-scale testing, migrate to a dedicated VPS. DCUtR remains the long-term solution to remove the relay from the data path.
