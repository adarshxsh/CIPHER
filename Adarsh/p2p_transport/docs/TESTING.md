# CIPHER P2P Testing Guide

This is a living document that outlines how to test the CIPHER P2P transport layer across all supported platforms and phases.

## Platform Support Matrix

| Platform | Role | Method | Notes |
|----------|------|--------|-------|
| macOS | Peer or Relay | `make run-peer1` / `make run-relay` | Native Go or compiled binary |
| Windows | Peer only | `.\cipher-peer.exe` | Cross-compiled from macOS. No Go/Make/Docker needed |
| Linux (Docker) | Peer or Relay | `docker-compose up` | For local multi-node testing |
| Render (Cloud) | Relay only | Auto-deploy from Git | WSS transport, `CIPHER_IDENTITY_KEY` env var |

## Quick Reference

### 1. Unit Tests
```bash
make test
```

### 2. Build Binaries
```bash
# Current platform
make build

# Cross-compile for Windows
make build-windows

# Cross-compile for Linux (Render/VPS)
make build-linux
```

### 3. Docker Compose (Local Multi-Node)
```bash
make docker-build
make docker-up
# Check logs
docker-compose logs -f
# Tear down
make docker-down
```

---

## Manual Testing

### Local Network Testing (Single Device or WiFi)

**Terminal 1 (Relay):**
```bash
make run-relay
```
*Copy the Relay ID from the logs.*

**Terminal 2 (Peer 1):**
```bash
make run-peer1 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>
```
*Copy the Peer 1 ID from the logs.*

**Terminal 3 (Peer 2):**
```bash
make run-peer2 RELAY=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID> TARGET=/ip4/127.0.0.1/tcp/9002/p2p/<RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

---

### Cloud Testing (Render Relay)

The relay is hosted on Render and communicates via **Secure WebSockets** (`/wss`) to bypass Render's HTTP-only load balancers.

First, copy `.env.example` to `.env` and fill in your Render URL and IDs to save typing.

**Terminal 1 (Peer 1 — macOS):**
```bash
make run-peer1 RELAY=/dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID>
```

**Terminal 2 (Peer 2 — Different network / Windows):**
```bash
# macOS
make run-peer2 RELAY=/dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID> TARGET=/dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>

# Windows (standalone binary)
.\cipher-peer.exe -listen 9001 -relay /dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID> -target /dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<CLOUD_RELAY_ID>/p2p-circuit/p2p/<PEER1_ID>
```

> **Render Cold Start**: The free tier spins down after 15 minutes of inactivity. First connection may take 30-60 seconds. The peer retries automatically with exponential backoff (2s → 4s → 8s → ... up to 60s, 10 attempts max).

---

### Verification Checklist

After any test run, verify these in the logs:

- [ ] Peer 1 logs: `Host started successfully` with correct Peer ID
- [ ] Peer 1 logs: `New incoming stream` from Peer 2
- [ ] Peer 1 logs: `Received message` — "Hello from \<Peer2 ID\>"
- [ ] Peer 2 logs: `Connected to peer` — Peer 1's ID
- [ ] Peer 2 logs: `Received reply` — "Hello from \<Peer1 ID\>"
- [ ] Health monitor logs: `Active Connection` with `type=Relayed` (Phase 2)
- [ ] No `context deadline exceeded` errors
- [ ] No `stream reset` errors

---

## Stress Testing

The stress test library is in `internal/testing/stress.go`. It provides reusable functions for validating transport reliability.

### Stress Test Scenarios

| Test | Duration | What to Watch For |
|------|----------|-------------------|
| **Peer Restart** | ~2 min | Kill peer → restart → verify reconnect within 30s |
| **Long Connection** | 10-60 min | Stream stays alive, no reservation expiry |
| **Transfer 100MB** | ~5 min | No memory leaks, no reconnects, throughput > 1MB/s |
| **Transfer 500MB** | ~15 min | Relay circuit data limit (128MB) may require multiple circuits |
| **Cross-Platform** | ~5 min | macOS ↔ Windows, verify identical behavior |

### Running Stress Tests

The stress tests currently require manual setup (two terminals). Automated harness coming in a future commit.

**Peer Restart Test:**
1. Start Peer 1 with relay
2. Start Peer 2, verify hello exchange
3. Kill Peer 1 (`Ctrl+C`)
4. Restart Peer 1
5. Start Peer 2 again — should reconnect within 30 seconds

**Long Connection Test:**
1. Start both peers with relay
2. Watch health monitor logs for 30+ minutes
3. Verify no `stream reset`, `reservation expired`, or `connection refused` errors

---

## Troubleshooting

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| `context deadline exceeded` on Connect | Relay is cold-starting (Render free tier) | Wait 30-60s and retry. The peer does this automatically |
| `context deadline exceeded` on NewStream | Ghost peer process running in background | Kill all peer processes: `pkill -f peer` |
| `failed to get relay reservation` | Relay not running or wrong Peer ID in address | Verify relay is up and Peer ID matches |
| `remote key matches previous one` | Render rolling deploy — old container still alive | Wait 30s for old container to drain |
| `failed to parse relay peer info` | Base64 key in multiaddress instead of Peer ID | Use `12D3KooW...` Peer ID, not the base64 private key |
| `no route to host` | Windows firewall blocking outbound TCP | Allow the `cipher-peer.exe` through Windows Firewall |

### Useful Debug Commands

```bash
# Kill all lingering peer processes
pkill -f "cipher-peer" || true
pkill -f "cmd/peer" || true

# Check if relay is reachable from your network
curl -s https://cipher-sk0s.onrender.com/health

# Monitor connections in real-time (logs print every 10s)
# Look for "Active Connection" entries with type=Relayed
```
