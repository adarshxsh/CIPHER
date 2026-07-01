# Production Considerations

When migrating this P2P Transport module from a local/testing environment to a live production environment, several critical changes and optimizations must be made to ensure security, performance, and stability.

## 1. Network & Reachability Configurations
During local testing, we explicitly forced the libp2p node to act as if it were behind a NAT using `libp2p.ForceReachabilityPrivate()`. 
- **Production Change:** Remove `libp2p.ForceReachabilityPrivate()`. 
- **Production Change:** Enable the **AutoNAT** service. AutoNAT allows peers to discover their own public reachability dynamically by asking other peers on the network.

## 2. Relay Node Deployment
The Relay node currently listens on `0.0.0.0:9002` with no resource limits.
- **Production Change:** The Relay node must be hosted on a statically accessible public IP address.
- **Production Change:** Implement resource limits. By default, `circuitv2` restricts relays, but you must tune the data limits, connection counts, and reservation TTLs to prevent DDoS attacks and excessive bandwidth consumption.

## 3. Identity Key Management
Currently, the identity keys (`peer1.key`, `peer2.key`, `relay.key`) are generated dynamically and stored as plain text files in the working directory.
- **Production Change:** Use secure key vaults or environment variables to inject the Ed25519 seed securely in production.
- **Production Change:** Do not rely on local disk storage in containerized/ephemeral environments unless mounted to a persistent, encrypted volume.

## 4. Hole Punching (Phase 3)
Relayed connections are expensive and slow (high latency, high bandwidth).
- **Production Change:** Enable `libp2p.EnableHolePunching()` (coming in Phase 3). This allows peers to use the Relay only for the initial handshake and coordinate a direct connection (TCP/UDP hole punching), dropping the Relay from the middle of the transfer.

## 5. Security & TLS
Currently, `go-libp2p` negotiates security by default (Noise / TLS), but we are binding to plain TCP ports.
- **Production Change:** Ensure that the host environment (AWS, GCP) correctly passes TCP traffic. You do not need an SSL certificate (like HTTPS) because `libp2p` handles its own encrypted handshakes over raw TCP using the Ed25519 identity keys.

## 6. Observability
We are currently using basic standard output `slog`.
- **Production Change:** Output logs in JSON format for easier ingestion by Datadog, Prometheus, or ELK stacks.
- **Production Change:** Integrate libp2p metrics (e.g., bandwidth counters, peer counts) and expose them via a `/metrics` Prometheus endpoint.
