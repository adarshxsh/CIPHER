#!/bin/bash
cd p2p_transport
pkill -9 -f "bin/peer" 2>/dev/null || true
pkill -9 -f "bin/publisher" 2>/dev/null || true
pkill -9 -f "bin/provider" 2>/dev/null || true
pkill -9 -f "bin/client" 2>/dev/null || true
pkill -9 -f "bin/bootstrap" 2>/dev/null || true
rm -rf store_a store_b test.mp4 out.mp4 peer_a.log peer_b.log
export CGO_ENABLED=0
export CIPHER_ALLOW_PRIVATE_DHT=1
go build -o bin/peer ./cmd/peer

echo "Testing plaintext transfer..." > test.mp4

./bin/peer -p 47891 -ws-port 0 -identity ./store_a/identity.key -store ./store_a -ingest test.mp4 > peer_a.log 2>&1 &
PEER_A_PID=$!

sleep 3

CONTENT_ID=$(strings peer_a.log | grep -a "ContentID:" | tail -n 1 | awk '{print $NF}')
KEY=$(strings peer_a.log | grep -a "Key:" | tail -n 1 | awk '{print $NF}')
ADDR=$(strings peer_a.log | grep -a "127.0.0.1/tcp/47891/p2p/" | head -n 1 | awk '{print $NF}')

echo "Content ID: $CONTENT_ID"
echo "Key:        $KEY"
echo "Address:    $ADDR"

./bin/peer -p 47892 -ws-port 0 -store ./store_b -identity ./store_b/identity.key -d "$ADDR" -fetch "$CONTENT_ID" -key "$KEY" -reassemble out.mp4 > peer_b.log 2>&1 &
PEER_B_PID=$!

for i in {1..15}; do
    if [ -f out.mp4 ]; then
        break
    fi
    sleep 1
done

kill $PEER_A_PID $PEER_B_PID 2>/dev/null || true
echo "--- out.mp4 ---"
cat out.mp4
