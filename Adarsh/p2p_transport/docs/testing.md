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

## Continuous Integration
Tests are intended to be executed automatically upon pull requests via standard CI pipelines to maintain code quality across iterations.
