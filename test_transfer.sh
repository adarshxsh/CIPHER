#!/bin/bash
cd p2p_transport
rm -rf store_a store_b test.mp4 out.mp4
export CGO_ENABLED=0
go build -o bin/peer ./cmd/peer

echo "Testing plaintext transfer..." > test.mp4

./bin/peer -p 55555 -store ./store_a -ingest test.mp4 > peer_a.log 2>&1 &
PEER_A_PID=$!

sleep 3

CONTENT_ID=$(grep "ContentID:" peer_a.log | awk '{print $NF}')
ADDR=$(grep "127.0.0.1/tcp/55555/p2p/" peer_a.log | head -n 1 | awk '{print $NF}')

echo "Content ID: $CONTENT_ID"
echo "Address: $ADDR"

./bin/peer -store ./store_b -d "$ADDR" -fetch "$CONTENT_ID" -reassemble out.mp4

kill $PEER_A_PID
echo "--- out.mp4 ---"
cat out.mp4
