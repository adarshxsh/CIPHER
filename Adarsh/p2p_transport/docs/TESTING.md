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

*Verifies that peers can connect via a Relay, and then upgrade that connection to a Direct Connection via Hole Punching (Phase 3).*

**Terminal 1 (Relay):**
```bash
make run-relay
```
*(Copy the Relay ID from the logs)*

**Terminal 2 (Peer 1):**
```bash
make run-peer1 -relay /ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>
```
*(Copy the Peer 1 ID from the logs)*

**Terminal 3 (Peer 2):**
```bash
make run-peer2 -relay /ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID> TARGET=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

**Verification:**
Watch the logs on Peer 1 and Peer 2. Every 10 seconds, the active connections are printed. 
1. Initially, you will see a connection with `type=Relayed`.
2. Shortly after, the Hole Punching sequence (DCUtR) will trigger, and you should see a new connection to the same peer with `type=Direct`.
