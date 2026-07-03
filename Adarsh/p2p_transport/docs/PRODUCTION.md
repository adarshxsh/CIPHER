# Production Considerations

When migrating this P2P Transport module from a local/testing environment to a live production environment, several critical changes and optimizations must be made to ensure security, performance, and stability.

## 1. Network & Reachability Configurations

During local testing, we explicitly forced the libp2p node to act as if it were behind a NAT using `libp2p.ForceReachabilityPrivate()`.

- **Production Change:** Remove `libp2p.ForceReachabilityPrivate()`.
- **Production Change:** Enable the **AutoNAT** service. AutoNAT allows peers to discover their own public reachability dynamically by asking other peers on the network.

## 2. Relay Node Deployment

### Current State (Development)

The relay is deployed on **Render Free Tier** at `cipher-sk0s.onrender.com`. It operates as a development and demonstration relay with the following characteristics:

| Property | Value | Notes |
|----------|-------|-------|
| Transport | WebSocket only (WSS on port 443) | Required by Render's HTTP load balancer |
| Identity | Persistent via `CIPHER_IDENTITY_KEY` env var | Survives Render container restarts |
| Max Reservations | 128 | Generous for demo scale |
| Max Circuits | 64 | Simultaneous active circuits |
| Circuit Lifetime | 5 minutes | Default is 2min — raised for file transfers |
| Circuit Data Limit | 128 MB | Default is much lower — raised for demo |
| Health Endpoint | `/health` on port 8080 | Returns JSON with peer/connection counts |
| Cold Start | 30-60 seconds after idle | Render free tier spins down after 15min inactivity |

### Production Migration

When moving to production-scale testing:
- Migrate to a **dedicated VPS** (e.g., DigitalOcean, AWS EC2) with a static IP
- Enable both **TCP and WebSocket** transports (VPS doesn't have Render's HTTP-only restriction)
- Tune resource limits based on expected peer count and file sizes
- Add Prometheus metrics endpoint for monitoring
- Consider running multiple relay nodes for redundancy (pass multiple addresses to `EnableAutoRelayWithStaticRelays`)

## 3. Identity Key Management

Currently, the identity keys (`peer1.key`, `peer2.key`, `relay.key`) are generated dynamically and stored as plain text files in the working directory.

- **Cloud Deployment:** The `CIPHER_IDENTITY_KEY` environment variable injects a base64-encoded persistent key. This prevents the Relay Peer ID from changing on every deploy (Render uses ephemeral storage).
- **Production Change:** Store keys in a secrets manager (e.g., AWS Secrets Manager, Vault) rather than environment variables.

## 4. Connection Management

### Timeout Configuration

All network operations now use timeout contexts instead of `context.Background()`:

| Operation | Timeout | Rationale |
|-----------|---------|-----------|
| `host.Connect()` | 30 seconds | Accounts for Render cold starts and relay circuit setup |
| `host.NewStream()` | 15 seconds | Stream negotiation should be fast once connected |

### Reconnection Strategy

The peer implements exponential backoff reconnection:

```
Attempt 1: wait 2s
Attempt 2: wait 4s
Attempt 3: wait 8s
...
Attempt N: wait min(2^N seconds, 60s)
Max attempts: 10
```

This handles:
- Render cold starts (30-60 second spin-up)
- Transient network failures
- Relay restarts

### Connection Manager

The libp2p connection manager prunes idle connections:
- **Low watermark**: 1 (minimum connections to maintain)
- **High watermark**: 10 (prune when above this)
- **Grace period**: 1 minute (new connections are immune)

## 5. Hole Punching (Phase 3) — Deferred

`libp2p.EnableHolePunching()` (DCUtR) is currently **disabled** on the `phase2-stable` branch.

**Why**: Opening a stream during DCUtR connection migration causes `context deadline exceeded` timeouts. The `time.Sleep(3s)` workaround is fragile and non-deterministic.

**Return plan**: Phase 3 will be revisited on a separate `phase3-holepunch` branch after:
1. Phase 2 transport is proven stable (stress tests pass)
2. CIPHER protocol works reliably over relayed connections
3. An event-driven connection state machine replaces timing workarounds

See `docs/ROADMAP.md` for the full plan.

## 6. Security & TLS

`go-libp2p` negotiates security by default (Noise / TLS), but we are binding to plain TCP ports.

- **Production Change:** Ensure that the host environment correctly passes TCP traffic. You do not need an SSL certificate (like HTTPS) because `libp2p` handles its own encrypted handshakes over raw TCP using the Ed25519 identity keys.
- **Cloud PaaS Limitations:** Platforms like Render or Heroku actively block raw TCP ports. In these environments, the Relay listens exclusively on WebSockets (WSS on port 443) to route traffic through their HTTP load balancers. TLS termination is handled by Render's edge proxy.

## 7. Observability

### Current
- `slog.TextHandler` with `LevelDebug` — human-readable structured logs
- Health monitor logs active connections every 10 seconds (peers: type, address, direction, stream count)
- Relay `/health` endpoint returns JSON: `{"status":"ok","peer_id":"...","peers":N,"connections":N}`

### Production Upgrades
- **JSON logging:** Switch `slog.TextHandler` to `slog.JSONHandler` for machine-parseable logs
- **Metrics:** Integrate libp2p bandwidth counters and expose via `/metrics` Prometheus endpoint
- **Alerting:** Alert on connection count drops, relay reservation failures, stream errors

## 8. Windows-Specific Considerations

Windows peers run as standalone executables (cross-compiled from macOS):

```bash
# Cross-compile on macOS
make build-windows
# Output: bin/cipher-peer.exe
```

**Known issues:**
- Windows Firewall may block outbound TCP connections — allow `cipher-peer.exe` through the firewall
- No Makefile support — run the binary directly with flags
- Key files use OS-specific paths — use relative paths or absolute paths with forward slashes
