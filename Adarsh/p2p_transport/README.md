# CIPHER P2P Transport

This repository contains the P2P transport module for the CIPHER project.

## Structure

- `cmd/peer`: Entry point for standard peers.
- `cmd/relay`: Entry point for relay peers.
- `internal/transport`: libp2p host creation and transport management.
- `internal/...`: Other modules placeholder (identity, protocol, crypto, chunk, merkle, packet).
- `configs/`: Configuration files.
- `data/`: Data storage directories.

## Documentation

- [Architecture](docs/architecture.md): Overview of the project's modular design and libp2p network topology.
- [Testing Strategy](docs/testing.md): Instructions and guidelines for unit and integration testing.
- [Relay Deployment](docs/relay_deployment.md): Instructions on how to deploy a public relay node on Linux.

## Setup

```bash
# Build the project into the bin directory
go build -o bin/peer ./cmd/peer
go build -o bin/relay ./cmd/relay

# Run tests
go test ./...
```
