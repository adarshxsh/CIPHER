# CIPHER P2P Transport

This repository contains the peer-to-peer transport layer for the CIPHER protocol.

## Phase 1: Identity & Local Loopback

In Phase 1, we establish a basic libp2p host with persistent Ed25519 identity. Two local processes can find each other and exchange a simple "Hello" message over a custom stream protocol (`/cipher/filetransfer/1.0.0`).

### Project Structure
- `internal/identity`: Manages generation and persistence of Ed25519 keys (`identity.key`).
- `internal/transport`: Sets up the raw `libp2p` host.
- `cmd/peer`: The CLI application for running a peer node.

### Manual Testing

1. **Start Peer 1**
   ```bash
   go run cmd/peer/main.go -listen 9000 -key peer1.key
   ```
   Take note of the `peer_id` in the output logs.

2. **Start Peer 2**
   In a separate terminal, run:
   ```bash
   go run cmd/peer/main.go -listen 9001 -key peer2.key -target /ip4/127.0.0.1/tcp/9000/p2p/<PEER1_ID>
   ```
   Replace `<PEER1_ID>` with the actual ID from Peer 1.

You should see both peers exchange a hello message and log it via `slog`.

### Docker Testing

A `docker-compose.yml` and `Dockerfile` are provided for testing in isolated containers.

1. **Build the image**
   ```bash
   docker-compose build
   ```

2. **Start Peer 1**
   ```bash
   docker-compose up peer1
   ```
   Note the `peer_id` logged by peer1.

3. **Update docker-compose.yml**
   Replace `PEER1_ID_PLACEHOLDER` in `docker-compose.yml` with the actual Peer 1 ID.

4. **Start Peer 2**
   ```bash
   docker-compose up peer2
   ```

You will see Peer 2 dial Peer 1, open a stream, and exchange the "Hello" message.
