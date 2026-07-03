# CIPHER P2P Testing Guide

This is a living document that outlines how to test the CIPHER P2P transport layer. We use a centralized `Makefile` to abstract away OS-level dependencies (like `CGO_ENABLED=0` required on macOS).

## Global Testing (All Phases)

### 1. Unit Tests
To run all unit tests for the project:
```bash
make test
```

### 2. Docker Compose Setup
We provide a `docker-compose.yml` that sets up all the nodes necessary to run the current phase.
```bash
make docker-build
make docker-up
```

## Manual Testing Commands

### Standalone Execution (No Docker)

Since Go compiles into a single executable file, you can run the peers entirely without Docker! You don't even need to install Go on the other device if you send them the compiled binary.

First, compile the applications:
```bash
go build -o cipher-relay ./cmd/relay
go build -o cipher-peer ./cmd/peer
```
Then run them using the binaries (e.g. `./cipher-peer -listen 9000 ...`).

---

### Local Network Testing (Single device or WiFi)

**Terminal 1 (Relay):**
```bash
make run-relay
```
*(Copy the Relay ID from the logs)*

**Terminal 2 (Peer 1):**
```bash
# Wait for the relay to fully start, then run:
make run-peer1 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>
```
*(Copy the Peer 1 ID from the logs)*

**Terminal 3 (Peer 2):**
```bash
make run-peer2 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID> TARGET=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

---

### Cloud Testing (Render / VPS)

If your Relay is hosted in the cloud (like Render), you must connect using **WebSockets** (`/wss`) to bypass HTTP-only load balancers. 

First, copy `.env.example` to `.env` and fill in your Render URL and IDs to save typing them out.

**Terminal 1 (Peer 1):**
```bash
# Connect to the cloud Relay using Secure WebSockets
make run-peer1 RELAY=/dns4/<YOUR_APP>.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID>
```

**Terminal 2 (Peer 2 on a different network):**
```bash
make run-peer2 RELAY=/dns4/<YOUR_APP>.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID> TARGET=/dns4/<YOUR_APP>.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

---

**Verification:**
Watch the logs on Peer 1 and Peer 2. Every 10 seconds, the active connections are printed. 
1. Initially, you will see a connection with `type=Relayed`.
2. Shortly after, the Hole Punching sequence (DCUtR) will trigger, and you should see a new connection to the same peer with `type=Direct`.
