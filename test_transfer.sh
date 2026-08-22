#!/bin/bash
cd p2p_transport
rm -rf store_a store_b test.mp4 out.mp4
export CGO_ENABLED=0
go build -o bin/peer ./cmd/peer

echo "Testing plaintext transfer..." > test.mp4

./bin/peer -p 47891 -ws-port 0 -identity ./store_a/identity.key -store ./store_a -ingest test.mp4 > peer_a.log 2>&1 &
PEER_A_PID=$!

sleep 3

CONTENT_ID=$(grep "ContentID:" peer_a.log | awk '{print $NF}')
KEY=$(grep "Key:" peer_a.log | awk '{print $NF}')
ADDR=$(grep "127.0.0.1/tcp/47891/p2p/" peer_a.log | head -n 1 | awk '{print $NF}')

echo "Content ID: $CONTENT_ID"
echo "Key:        $KEY"
echo "Address:    $ADDR"

./bin/peer -p 47892 -ws-port 0 -store ./store_b -identity ./store_b/identity.key -d "$ADDR" -fetch "$CONTENT_ID" -key "$KEY" -reassemble out.mp4

kill $PEER_A_PID 2>/dev/null || true
echo "--- out.mp4 ---"
cat out.mp4
