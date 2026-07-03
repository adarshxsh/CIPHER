# Makefile for CIPHER P2P Transport
# Supports macOS, Linux, and cross-compilation to Windows.

# Go settings
GO=go
CGO_ENABLED=0

# Default keys
KEY1=keys/peer1.key
KEY2=keys/peer2.key

# Default stress test parameters
STRESS_SIZE=104857600  # 100 MB
STRESS_DURATION=600    # 10 minutes (seconds)

.PHONY: all build build-windows test clean docker-build docker-up docker-down \
	run-peer1 run-peer2 run-relay stress-transfer stress-longconn stress-all

all: build

# ──────────────────────────────────────────────
# Build
# ──────────────────────────────────────────────

# Build binaries for current OS
build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/peer ./cmd/peer
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/relay ./cmd/relay

# Cross-compile peer binary for Windows (to send to Windows testers)
build-windows:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=windows GOARCH=amd64 $(GO) build -o bin/cipher-peer.exe ./cmd/peer

# Cross-compile relay binary for Linux (for Render/VPS deployment)
build-linux:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=amd64 $(GO) build -o bin/relay_linux ./cmd/relay

# ──────────────────────────────────────────────
# Test
# ──────────────────────────────────────────────

# Run all unit tests
test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./... -v

# Run tests with race detector (requires CGO on some platforms)
test-race:
	$(GO) test ./... -v -race

# ──────────────────────────────────────────────
# Clean
# ──────────────────────────────────────────────

clean:
	rm -rf bin/
	rm -rf keys/
	rm -rf data/

# ──────────────────────────────────────────────
# Docker
# ──────────────────────────────────────────────

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# ──────────────────────────────────────────────
# Manual Run Shortcuts
# ──────────────────────────────────────────────

# Run Peer 1 (Receiver). Waits for incoming connections.
# Usage: make run-peer1 RELAY=/dns4/cipher-sk0s.onrender.com/tcp/443/wss/p2p/<RELAY_ID>
run-peer1:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/peer/main.go -listen 9000 -key $(KEY1) $(if $(RELAY),-relay $(RELAY))

# Run Peer 2 (Sender). Dials target and exchanges hello.
# Usage: make run-peer2 RELAY=/ip4/.../p2p/<RELAY_ID> TARGET=/ip4/.../p2p-circuit/p2p/<PEER1_ID>
run-peer2:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/peer/main.go -listen 9001 -key $(KEY2) $(if $(RELAY),-relay $(RELAY)) -target $(TARGET) $(if $(SEND),-send $(SEND))

# Run the Relay node locally
run-relay:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/relay/main.go -listen 9002 -key keys/relay.key
