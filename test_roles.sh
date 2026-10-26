#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR/p2p_transport"

echo "=========================================================="
echo "         CIPHER Role-Based Architecture Test             "
echo "=========================================================="

rm -rf store_publisher store_provider store_client test_input.dat test_output.dat test_output_dht.dat
rm -f publisher.log provider.log client.log bootstrap.log

export CGO_ENABLED=0

echo "[1/4] Building binaries (publisher, provider, client, bootstrap)..."
go build -o bin/publisher ./cmd/publisher
go build -o bin/provider ./cmd/provider
go build -o bin/client ./cmd/client
go build -o bin/bootstrap ./cmd/bootstrap

echo "[2/4] Generating test payload..."
head -c 1048576 </dev/urandom > test_input.dat
ORIGINAL_HASH=$(shasum -a 256 test_input.dat | awk '{print $1}')
echo "Payload SHA-256: $ORIGINAL_HASH (1 MB)"

echo "[3/5] Starting Publisher to ingest and seed content..."
./bin/publisher -p 45001 -ws-port 45002 -identity ./store_publisher/identity.key -store ./store_publisher -file test_input.dat > publisher.log 2>&1 &
PUB_PID=$!

cleanup() {
    kill $PUB_PID $BOOT_PID $PROV_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 2

CONTENT_ID=$(grep "^ContentID" publisher.log | awk '{print $NF}')
KEY=$(grep "^Decryption Key" publisher.log | awk '{print $NF}')
PUB_ADDR=$(grep "127.0.0.1/tcp/45001/p2p/" publisher.log | head -n 1 | awk '{print $NF}')

if [ -z "$CONTENT_ID" ] || [ -z "$KEY" ] || [ -z "$PUB_ADDR" ]; then
    echo "Error: Failed to parse publisher parameters from log:"
    cat publisher.log
    exit 1
fi

echo "  - ContentID: $CONTENT_ID"
echo "  - Key:       $KEY"
echo "  - Address:   $PUB_ADDR"

echo "[4/5] Running Client to fetch, verify, and reassemble content..."
./bin/client -p 55001 -ws-port 55002 -identity ./store_client/identity.key -store ./store_client -d "$PUB_ADDR" -fetch "$CONTENT_ID" -key "$KEY" -out test_output.dat > client.log 2>&1

DOWNLOADED_HASH=$(shasum -a 256 test_output.dat | awk '{print $1}')
echo "Downloaded SHA-256: $DOWNLOADED_HASH"

if [ "$ORIGINAL_HASH" != "$DOWNLOADED_HASH" ]; then
    echo "❌ FAILED: Hash mismatch between original and downloaded payload!"
    exit 1
fi

echo "[5/6] Checking session status..."
./bin/client -store ./store_client -status || true

echo "[6/6] Testing Control Plane (DHT Provider Discovery)..."
# Start bootstrap node
./bin/bootstrap -p 45003 -identity ./bootstrap_identity.key > bootstrap.log 2>&1 &
BOOT_PID=$!
sleep 1
BOOT_ADDR=$(grep "127.0.0.1/tcp/45003/p2p/" bootstrap.log | head -n 1 | awk '{print $NF}')
echo "  - Bootstrap Node: $BOOT_ADDR"

# Start Provider with bootstrap connection
./bin/provider -p 45010 -ws-port 45011 -identity ./store_provider/identity.key -store ./store_publisher -bootstrap "$BOOT_ADDR" > provider.log 2>&1 &
PROV_PID=$!
sleep 2

# Run Client using ONLY DHT discovery (no direct -d flag)
./bin/client -p 55010 -ws-port 55011 -identity ./store_client2/identity.key -store ./store_client2 -bootstrap "$BOOT_ADDR" -fetch "$CONTENT_ID" -key "$KEY" -out test_output_dht.dat > client_dht.log 2>&1

DHT_DOWNLOADED_HASH=$(shasum -a 256 test_output_dht.dat | awk '{print $1}')
echo "DHT Downloaded SHA-256: $DHT_DOWNLOADED_HASH"

if [ "$ORIGINAL_HASH" != "$DHT_DOWNLOADED_HASH" ]; then
    echo "❌ FAILED: Hash mismatch in DHT-discovered transfer!"
    exit 1
fi

kill $BOOT_PID $PROV_PID 2>/dev/null || true

echo "✅ SUCCESS: Payload downloaded, verified, and reassembled identically across roles (both direct & DHT)!"
echo "=========================================================="
