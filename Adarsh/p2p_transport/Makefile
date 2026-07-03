# Makefile for CIPHER P2P Transport

# Go settings
GO=go
CGO_ENABLED=0

# Default keys
KEY1=keys/peer1.key
KEY2=keys/peer2.key

.PHONY: all build test clean docker-build docker-up docker-down run-peer1 run-peer2 run-relay

all: build

# Build the binaries
build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/peer ./cmd/peer
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -o bin/relay ./cmd/relay

# Run tests with CGO disabled (to avoid macOS Xcode issues)
test:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) test ./... -v

# Clean up binaries and keys
clean:
	rm -rf bin/
	rm -rf keys/
	rm -rf data/

# Docker shortcuts
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# Manual Run Shortcuts
run-peer1:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/peer/main.go -listen 9000 -key $(KEY1) $(if $(RELAY),-relay $(RELAY))

# Usage: make run-peer2 TARGET=/ip4/127.0.0.1/tcp/9000/p2p/PEER1_ID RELAY=/ip4/...
run-peer2:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/peer/main.go -listen 9001 -key $(KEY2) $(if $(RELAY),-relay $(RELAY)) -target $(TARGET)

run-relay:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) run ./cmd/relay/main.go -listen 9002 -key keys/relay.key
