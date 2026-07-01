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

*Verifies that peers can connect by routing their traffic through a central Relay node.*

**Terminal 1 (Relay):**
```bash
make run-relay
```
*(Copy the Relay ID from the logs)*

**Terminal 2 (Peer 1):**
```bash
make run-peer1
```
*(Copy the Peer 1 ID from the logs)*

**Terminal 3 (Peer 2):**
```bash
make run-peer2 TARGET=/ip4/127.0.0.1/tcp/9000/p2p/<PEER1_ID>
```
*(Note: To test pure relay routing locally without a direct dial fallback, you can modify the Makefile `TARGET` to use the relayed multiaddress: `/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>`)*
