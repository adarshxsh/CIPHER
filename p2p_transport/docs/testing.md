# CIPHER Testing Strategy & Quick Start Guide

This document provides a modern, fast, and structured guide to testing the CIPHER decentralized content delivery network. It covers unit testing, automated end-to-end role validation, multi-provider lifecycle, relay/DCUtR testing, and manual verification workflows.

---

## ⚡ Quick Start: Fast Automated Testing

All tests can be run locally with CGO disabled:

```bash
# 1. Run all unit and robustness tests (~3s)
CGO_ENABLED=0 go test -count=1 ./test/robustness/... ./internal/...

# 2. Run automated End-to-End Role Separation Test (Publisher, Provider, Client, Bootstrap)
bash test_roles.sh

# 3. Run Single-Provider Independence & Persistence Restart Test
bash test_provider_independent.sh

# 4. Run Legacy Peer A <-> Peer B transfer test
bash test_transfer.sh
```

---

## 🏗️ The 5 CIPHER Roles Under Test

| Role | Binary Path | Responsibilities |
| :--- | :--- | :--- |
| **Publisher** | `cmd/publisher` | Ingests source file, chunks & encrypts via XChaCha20, creates immutable manifest, advertises `ContentID` to DHT, and seeds. |
| **Provider** | `cmd/provider` | Standalone daemon hosting Content-Addressed Storage (CAS), continuously republishes manifests to DHT (`StartRepublisher`), and serves `/cipher/chunk/1.0.0`. |
| **Client** | `cmd/client` | Discovers providers via DHT (or direct dial `-d`), resolves manifest, downloads chunks concurrently via worker pool, verifies hashes, decrypts and reassembles payload. |
| **Bootstrap** | `cmd/bootstrap` | Kademlia DHT bootstrap routing node for decentralized provider discovery. |
| **Relay** | `cmd/relay` | Circuit v2 Relay node for NAT traversal and DCUtR hole punching coordination. |

---

## 🧪 Core Test Scenarios

### Scenario 1: Independent Provider Lifecycle (Publisher Offline)
**Objective**: Prove that once content is published to a Provider, the **Publisher is completely removed from the retrieval path**.

```text
[Step 1] Bootstrap Node starts (DHT mesh coordinator)
[Step 2] Publisher ingests 2 MB test file into Provider CAS store
[Step 3] Publisher process is completely KILLED (verified 100% offline)
[Step 4] Provider starts independently with existing CAS store & registers with DHT
[Step 5] Client queries DHT solely with ContentID + Key + Bootstrap address
         - Discovers Provider on DHT (no direct peer address passed)
         - Resolves Manifest from Provider over /cipher/chunk/1.0.0
         - Downloads all chunks in parallel & decrypts payload
         - Validates SHA-256 checksum equality (100% match)
```

**Automated Command**:
```bash
bash test_provider_independent.sh
```

---

### Scenario 2: Provider Persistence & Restart
**Objective**: Prove that a Provider can be stopped, restarted, and immediately re-announce its local CAS store without data corruption or manifest loss.

```text
[Step 1] Stop the active Provider (kill $PROV_PID)
[Step 2] Restart Provider pointing to existing store directory (-store ./provider_store)
[Step 3] Provider startup automatically triggers discovery.StartRepublisher (re-announcing all CIDs)
[Step 4] A new Client queries DHT, discovers restarted Provider, fetches chunks, and reassembles
```

*This is automatically validated as Step 6 in `test_provider_independent.sh`.*

---

### Scenario 3: Multi-Provider Swarming (Concurrent Chunk Fetching)
**Objective**: Prove that the Client's `TransferManager` and `Scheduler` parallelize chunk downloads across multiple independent providers.

```text
                     DHT (Control Plane)
                              │
                    FindProviders(ContentID)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
               Provider A          Provider B
                (Seed 1)            (Seed 2)
                    │                   │
                    │   /cipher/chunk   │
                    └───►   Client  ◄───┘
```

#### Manual Walkthrough:
1. **Start Bootstrap**:
   ```bash
   go run cmd/bootstrap/main.go -p 4003 -identity ./store_boot/boot.key
   ```
2. **Publish Content**:
   ```bash
   go run cmd/publisher/main.go -file video.mp4 -store ./store_shared -seed=false
   ```
3. **Start Provider A (Port 4001)**:
   ```bash
   go run cmd/provider/main.go -p 4001 -ws-port 4002 -store ./store_shared -identity ./store_a/a.key -bootstrap "<BOOTSTRAP_MULTIADDR>"
   ```
4. **Start Provider B (Port 4010)**:
   ```bash
   go run cmd/provider/main.go -p 4010 -ws-port 4011 -store ./store_shared -identity ./store_b/b.key -bootstrap "<BOOTSTRAP_MULTIADDR>"
   ```
5. **Run Client with DHT Discovery**:
   ```bash
   go run cmd/client/main.go -bootstrap "<BOOTSTRAP_MULTIADDR>" -fetch "<CONTENT_ID>" -key "<KEY>" -out downloaded.mp4
   ```
6. **Observe Swarm Metrics**:
   The client logs will display chunk contributions divided between Provider A and Provider B:
   ```text
   --- Peer Contribution Metrics ---
   Peer 12D3KooW... (Provider A): 16 chunks (50.0%)
   Peer 12D3KooX... (Provider B): 16 chunks (50.0%)
   ---------------------------------
   ```

---

### Scenario 4: Provider Failure & Dynamic Re-scheduling
**Objective**: Prove that killing one Provider midway through a download triggers the Scheduler retry policy, transparently redirecting remaining chunk tasks to surviving providers.

1. Initiate a large file transfer (e.g., 50MB) with Provider A and Provider B active.
2. Kill Provider A midway (`Ctrl+C` or `kill -9`).
3. **Expected Behavior**:
   - Active worker requests to Provider A time out / error.
   - `Scheduler` catches the failure, marks Provider A unavailable, and pushes pending `ChunkTask`s back into the queue.
   - Surviving worker connected to Provider B fetches the remaining chunks.
   - Client reassembles the file without error.

---

### Scenario 5: Public Relay & DCUtR Hole Punching (NAT Traversal)
**Objective**: Connect across NATs/firewalls via a public `circuitv2` relay and verify automatic, seamless upgrade to direct TCP/UDP sockets via DCUtR.

1. **Start Public Relay (on cloud VM / Azure / Ubuntu)**:
   ```bash
   go run cmd/relay/main.go
   ```
   *Copy the relay multiaddress:* `/ip4/<PUBLIC_IP>/tcp/4001/p2p/<RELAY_PEER_ID>`
2. **Start Provider behind NAT**:
   ```bash
   go run cmd/provider/main.go -p 4001 -store ./provider_store -relay "/ip4/<PUBLIC_IP>/tcp/4001/p2p/<RELAY_ID>" -bootstrap "<BOOTSTRAP_ADDR>"
   ```
3. **Start Client on separate machine / network**:
   ```bash
   go run cmd/client/main.go -relay "/ip4/<PUBLIC_IP>/tcp/4001/p2p/<RELAY_ID>" -bootstrap "<BOOTSTRAP_ADDR>" -fetch "<CID>" -key "<KEY>" -out output.mp4
   ```
4. **Verify DCUtR Upgrade**:
   - Initial connection is established over the limited relay circuit.
   - DCUtR executes simultaneous hole punch in the background (`[DCUtR] Hole Punch Event: StartHolePunch` $\rightarrow$ `EndHolePunch`).
   - Transfer shifts to high-throughput direct socket (`Path: Direct`).

---

## 📊 Unit & Robustness Test Suites

### Content Engine Suite
```bash
CGO_ENABLED=0 go test -v ./internal/content/...
```
- **Chunking**: Fixed/dynamic slicing (32KB/256KB).
- **Crypto**: XChaCha20-Poly1305 in-place chunk encryption with unique 192-bit nonces.
- **Integrity**: SHA-256 ciphertext content-addressing (`[32]byte`).
- **Manifest**: Serialization/deserialization of immutable capability files.
- **CAS Store**: Sharded directory hashing (`store/ab/cd/...`).

### Chunk Wire Protocol Suite
```bash
CGO_ENABLED=0 go test -v ./internal/protocol/chunk/...
```
- **Message Encoding**: Binary symmetric envelope validation (Version, Type, Payload).
- **Stream Handlers**: Request-response handling for `REQUEST_MANIFEST`, `MANIFEST`, `REQUEST_CHUNK`, `CHUNK`, `ACK`, `ERROR`.
- **Integrity Rejection**: Corrupt payloads immediately rejected before hitting disk.

### 1000-Iteration Robustness Gauntlet
```bash
CGO_ENABLED=0 go test -v ./test/robustness/...
```
Generates random binary payloads, randomized chunk sizes, randomized encryption keys, ingests, shuffles chunks, reconstructs, and validates SHA-256 byte-for-byte identity across 1000 iterations.

---

## 🛠️ CLI Quick Reference

```bash
# Publisher: Ingest file and print CID + Key
go run cmd/publisher/main.go -file <file> -store <store_dir> -seed=false

# Provider: Start long-running storage node
go run cmd/provider/main.go -p 4001 -ws-port 4002 -store <store_dir> -bootstrap <boot_multiaddr>

# Client: Fetch content via DHT and reassemble
go run cmd/client/main.go -bootstrap <boot_multiaddr> -fetch <CID> -key <KEY_HEX> -out <output_path>

# Client: Query active download sessions
go run cmd/client/main.go -store <store_dir> -status

# Client: Cancel a download session
go run cmd/client/main.go -store <store_dir> -cancel <CID>
```
