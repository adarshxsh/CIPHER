#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR/p2p_transport"

echo "======================================================================"
echo "    CIPHER Lifecycle Test: Provider Independence & Persistence       "
echo "======================================================================"

rm -rf store_provider store_client1 store_client2
rm -f test_orig.dat test_recov1.dat test_recov2.dat
rm -f bootstrap.log publisher.log provider.log client1.log client2.log

export CGO_ENABLED=0

cleanup() {
    kill $BOOT_PID $PUB_PID $PROV_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "\n[Step 1/6] Building binaries..."
go build -o bin/bootstrap ./cmd/bootstrap
go build -o bin/publisher ./cmd/publisher
go build -o bin/provider ./cmd/provider
go build -o bin/client ./cmd/client

echo "\n[Step 2/6] Generating 2 MB test payload..."
head -c 2097152 </dev/urandom > test_orig.dat
ORIG_HASH=$(shasum -a 256 test_orig.dat | awk '{print $1}')
echo "Original Payload SHA-256: $ORIG_HASH"

echo "\n[Step 3/6] Starting DHT Bootstrap Node..."
./bin/bootstrap -p 48001 -ws-port 0 -identity ./store_provider/boot.key > bootstrap.log 2>&1 &
BOOT_PID=$!
sleep 1

BOOT_ADDR=$(grep "127.0.0.1/tcp/48001/p2p/" bootstrap.log | head -n 1 | awk '{print $NF}')
echo "Bootstrap Multiaddr: $BOOT_ADDR"

echo "\n[Step 4/6] Publisher ingests content into Provider store and EXITS..."
# Seed is set to false so the publisher explicitly terminates after ingestion
./bin/publisher -seed=false -identity ./store_provider/pub.key -store ./store_provider -file test_orig.dat > publisher.log 2>&1

CONTENT_ID=$(grep "^ContentID" publisher.log | awk '{print $NF}')
KEY=$(grep "^Decryption Key" publisher.log | awk '{print $NF}')

echo "Content Published:"
echo "  - ContentID:      $CONTENT_ID"
echo "  - Decryption Key: $KEY"

# Verify publisher process is dead
if pgrep -f "cmd/publisher" > /dev/null; then
    echo "Error: Publisher is still running!"
    exit 1
fi
echo "✓ Verified: Publisher process is completely offline."

echo "\n[Step 5/6] Starting Standalone Provider..."
./bin/provider -p 48010 -ws-port 48011 -identity ./store_provider/prov.key -store ./store_provider -bootstrap "$BOOT_ADDR" > provider.log 2>&1 &
PROV_PID=$!
sleep 2

PROV_ID=$(grep "Provider Peer ID:" provider.log | awk '{print $NF}')
echo "Provider running with Peer ID: $PROV_ID"

echo "\nRunning Client 1 (DHT Discovery only — NO Publisher in network)..."
./bin/client -p 48020 -ws-port 0 -identity ./store_client1/client.key -store ./store_client1 -bootstrap "$BOOT_ADDR" -fetch "$CONTENT_ID" -key "$KEY" -out test_recov1.dat > client1.log 2>&1

RECOV1_HASH=$(shasum -a 256 test_recov1.dat | awk '{print $1}')
echo "Client 1 Downloaded SHA-256: $RECOV1_HASH"

if [ "$ORIG_HASH" != "$RECOV1_HASH" ]; then
    echo "❌ FAILED: Client 1 hash mismatch!"
    exit 1
fi
echo "✓ SUCCESS: Client 1 retrieved file solely via DHT discovery from independent Provider!"

echo "\n[Step 6/6] Testing Provider Persistence: Killing Provider & Restarting..."
kill $PROV_PID
wait $PROV_PID 2>/dev/null || true
echo "✓ Provider stopped."

sleep 1

echo "Restarting Provider from existing CAS store..."
./bin/provider -p 48010 -ws-port 48011 -identity ./store_provider/prov.key -store ./store_provider -bootstrap "$BOOT_ADDR" > provider_restart.log 2>&1 &
PROV_PID=$!
sleep 2

echo "Running Client 2 against restarted Provider via DHT..."
./bin/client -p 48030 -ws-port 0 -identity ./store_client2/client.key -store ./store_client2 -bootstrap "$BOOT_ADDR" -fetch "$CONTENT_ID" -key "$KEY" -out test_recov2.dat > client2.log 2>&1

RECOV2_HASH=$(shasum -a 256 test_recov2.dat | awk '{print $1}')
echo "Client 2 Downloaded SHA-256: $RECOV2_HASH"

if [ "$ORIG_HASH" != "$RECOV2_HASH" ]; then
    echo "❌ FAILED: Client 2 hash mismatch after Provider restart!"
    exit 1
fi
echo "✓ SUCCESS: Restarted Provider successfully loaded CAS manifests and served Client 2!"

echo "\n======================================================================"
echo "    🎉 ALL LIFECYCLE TESTS PASSED (Provider Independence + Persistence) "
echo "======================================================================"
