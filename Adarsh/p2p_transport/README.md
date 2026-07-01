# CIPHER P2P Transport

This repository contains the peer-to-peer transport layer for the CIPHER protocol.

## Phase 1: Identity & Local Loopback

In Phase 1, we establish a basic libp2p host with persistent Ed25519 identity. Two local processes can find each other and exchange a simple "Hello" message over a custom stream protocol (`/cipher/filetransfer/1.0.0`).

### Project Structure
- `internal/identity`: Manages generation and persistence of Ed25519 keys (`identity.key`).
- `internal/transport`: Sets up the raw `libp2p` host.
- `cmd/peer`: The CLI application for running a peer node.

### Manual & Docker Testing

For detailed instructions on how to run tests locally (via standard commands or Docker) across all phases, please see [docs/TESTING.md](docs/TESTING.md).

## Phase 2: Relay Bootstrap

In Phase 2, we introduce a lightweight Relay node (`cmd/relay`) and enable `libp2p.EnableRelayService()`. 
Peers can connect to each other by routing their traffic through this central Relay node, bypassing NATs temporarily before we implement hole punching.
