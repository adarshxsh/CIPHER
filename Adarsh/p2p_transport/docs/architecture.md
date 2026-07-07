# CIPHER P2P Architecture

## Overview
The CIPHER project is built on top of [libp2p](https://libp2p.io/), utilizing a modular approach to separate concerns across the transport, identity, cryptography, and protocol layers.

## Modules

### `cmd/` (Entrypoints)
- **peer**: The standard client node participating in the network.
- **relay**: A specialized node designed to relay traffic between peers that may be behind NATs or firewalls.

### `internal/` (Core Logic)
- **transport**: Manages the initialization of libp2p hosts, connection establishment, and multiplexing.
- **identity**: Handles peer ID generation, key management, and cryptographic identities. It includes a persistent identity system that ensures a node's `PeerID` remains constant across restarts by storing an Ed25519 private key in the user's OS-level configuration directory (`~/.config/cipher/`, `Library/Application Support/CIPHER/`, or `AppData/Roaming/CIPHER/`).
- **crypto**: Provides standard cryptographic primitives for the broader application.
- **protocol**: Defines the custom network protocols used by CIPHER, currently including `/cipher/filetransfer/1.0.0` for direct P2P communication.
- **chunk** & **packet**: Handles the chunking and packetization of data for efficient transmission.
- **merkle**: Implements Merkle trees for data verification and integrity checks.

## Network Topology
The network utilizes a hybrid peer-to-peer topology where standard peers connect to one another directly if possible, or fallback to utilizing `relay` nodes for NAT traversal and connectivity routing.

### Relay Connectivity Strategy (Circuit v2)
CIPHER leverages go-libp2p's `circuitv2` protocol for relaying traffic. By default, `circuitv2` establishes **limited** (transient) connections designed strictly for lightweight coordination protocols (like DCUtR Hole Punching), rather than bulk application data transfer. 
- During **Milestone 5 (Relay Validation)**, CIPHER explicitly permits the custom file transfer protocol (`/cipher/filetransfer/1.0.0`) to run over limited relay connections using `network.WithAllowLimitedConn`.
- During **Milestone 6 (Hole Punching)**, this explicit permission will be removed, as the DCUtR subsystem will automatically upgrade the limited relay connection into a direct, non-limited TCP/UDP connection via UDP hole punching, drastically improving throughput and latency.

```mermaid
graph TD
    subgraph Direct Connection
        PeerA["Peer A (Listener)"] <-->|"/cipher/filetransfer/1.0.0"| PeerB["Peer B (Dialer)"]
    end
    subgraph Relayed Connection
        PeerC["Peer C (NAT)"] -->|Circuit v2 Reservation| Relay["Relay Node"]
        Relay -->|Relayed Stream| PeerD["Peer D (NAT)"]
    end
```
