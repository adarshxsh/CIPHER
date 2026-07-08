# Testing Strategy

The CIPHER project employs a comprehensive testing strategy to ensure reliability and correctness of the P2P networking components.

## Running Tests Locally

To run all unit tests for the project without CGO dependencies:

```bash
CGO_ENABLED=0 go test ./...
```

To run tests with verbose output:

```bash
CGO_ENABLED=0 go test -v ./...
```

## Testing Layers

1. **Unit Testing**:
   - Focuses on testing individual modules in isolation.
   - Example: Testing the `transport.NewNode` initialization ensures that a libp2p host can be correctly allocated and bound to a port.

2. **Integration Testing** (Planned):
   - Focuses on testing interactions between the `peer` and `relay` nodes.
   - Verifies end-to-end connectivity, stream multiplexing, and protocol correctness over local temporary networks.

3. **Manual Testing**:
   - **Persistent Identity**: To manually test that node identities persist across restarts:
     1. Build the peer binary: `go build -o bin/peer ./cmd/peer`
     2. Run the peer node: `./bin/peer`
     3. Note the outputted `Peer ID` and shut down the peer (Ctrl+C).
     4. Run the peer node again: `./bin/peer`
     5. Verify that the outputted `Peer ID` matches exactly with the previous run.

   - **Direct P2P Connection (File Transfer)**: To manually test the `/cipher/filetransfer/1.0.0` protocol:
     1. Build the peer binary: `go build -o bin/peer ./cmd/peer`
     2. Start **Peer A (Listener)** on a specific port (e.g., 55555): `./bin/peer -p 55555`
     3. Take note of Peer A's full multiaddress, printed in the logs (e.g., `/ip4/127.0.0.1/tcp/55555/p2p/12D3...`).
     4. Open a second terminal window. Start **Peer B (Dialer)**, passing Peer A's address using the `-d` flag, and overriding its config directory so it generates a new identity: `CIPHER_CONFIG_DIR=/tmp/cipher-peer-b ./bin/peer -d /ip4/127.0.0.1/tcp/55555/p2p/12D3...`
     5. Observe Peer A's logs: It should log `Got a new stream from ...`, `Received: hello`, and `Sending hello back...`.
     6. Observe Peer B's logs: It should log `Connected to ..., sending hello...` and then `Received: hello back`.

   - **Relay Node Deployment**: To manually test the relay functionality (without full NAT traversal logic yet, just connection):
     1. Build the relay binary: `go build -o bin/relay ./cmd/relay`
     2. Start the relay: `./bin/relay`
     3. Ensure it outputs `Relay Service Started!` and prints its multiaddresses (e.g., `/ip4/127.0.0.1/tcp/4001/p2p/...`).
     4. To verify routing traffic, proceed to the **Relay Connectivity** step below.

   - **Relay Connectivity & File Transfer**: To verify that a peer (e.g., Mac) can connect to another peer (e.g., Windows) through a public relay (e.g., Ubuntu server) and transfer a file:
     1. Start your public relay node on the Ubuntu server and copy its multiaddress (e.g., `/ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>`).
     2. On **Peer A** (Listener, e.g., Windows), start the peer and configure it to use the static relay:
        `./bin/peer -p 55555 -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>`
     3. Take note of Peer A's generated Relay Multiaddress from its logs (it will end in `/p2p-circuit/p2p/<PEER-A-ID>`).
     4. On **Peer B** (Dialer, e.g., Mac), create a test file (e.g., `dd if=/dev/urandom of=test.bin bs=1M count=5`).
     5. Start Peer B, provide the static relay, dial Peer A's relayed address, and specify the file to send:
        `./bin/peer -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -d /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>/p2p-circuit/p2p/<PEER-A-ID> -send test.bin`
     6. **Verify Hole Punching**: After the initial relayed connection, both peers should log `[DCUtR] Hole Punch Event: StartHolePunch`. Following a successful attempt, you should see `EndHolePunch`, and the `[Network] Active connections to ...` log will enumerate a newly established `Direct` connection (e.g., `/ip4/.../tcp/...` without the `/p2p-circuit` postfix). Subsequent streams will automatically route over this direct connection.
     7. **Verify Transfer Integrity**: Peer B will log `Sending: test.bin` with progress tracking. Peer A will automatically download the file to the `downloads/` directory, compute its SHA-256 hash, compare it against the sender's hash, and log `Integrity: VERIFIED` along with the transfer duration and throughput.

   - **Relay Fallback**: To verify that the system can still transfer data over the relay when direct connection fails (or is disabled):
     1. Start **Peer A** with the new `-force-relay` flag to disable hole punching:
        `./bin/peer -p 55555 -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -force-relay`
     2. Start **Peer B** with the `-force-relay` flag as well:
        `./bin/peer -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -d /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>/p2p-circuit/p2p/<PEER-A-ID> -send test.bin -force-relay`
     3. Verify that the file transfers successfully. The logs should explicitly say `Path: Relay` instead of `Path: Direct`.

   - **Direct Upgrade (Hole Punching)**: To verify natural direct upgrade:
     1. Start Peer A and B with a relay, but without the `-force-relay` flag.
     2. Start a large transfer (e.g. 50-100MB).
     3. During the transfer, you should observe `[DCUtR] Hole Punch Event: StartHolePunch` and `EndHolePunch`. 
     4. Note that for a single stream, the underlying connection used is decided when the stream is created. Any *new* stream after successful hole punching will use the direct connection (you will see `Path: Direct` in the transfer logs). For large transfers initiated before hole punch completes, they may finish over Relay depending on the exact libp2p stream multiplexer routing. Wait for `EndHolePunch` to finish and initiate another transfer to verify it says `Path: Direct` and is much faster.

   - **Integrity Verification**: 
     1. This is automatically tested on every transfer. 
     2. Verify that on the receiving end (Peer A), the log prints `Integrity: VERIFIED` at the end of the transfer. If a file gets corrupted, it will print `[WARNING] Checksum mismatch!`.

## Transport Layer Test Matrix

Before moving to the final CIPHER chunking protocol (Milestone 8), the transport layer is considered validated only after all the following manual tests pass:

- [☑️] **Small Payload**: 1 KB file transfer
- [☑️] **Medium Payload**: 1 MB file transfer
- [☑️] **Large Payload**: 50–100 MB file transfer
- [☑️] **Binary Format**: e.g., ZIP or JPEG file
- [☑️] **Bidirectional**: Transfer A→B and B→A
- [☑️] **Relay Fallback**: Transfer over relay only (disable DCUtR / hole punching)
- [☑️] **Direct Upgrade**: Transfer naturally switches to direct path after successful DCUtR
- [☑️] **Integrity**: SHA-256 verification succeeds for every single transfer

## Continuous Integration
Tests are intended to be executed automatically upon pull requests via standard CI pipelines to maintain code quality across iterations.
