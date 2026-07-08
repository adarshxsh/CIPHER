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

   - **Direct P2P Connection (Chunk Transport)**: To manually test the `/cipher/chunk/1.0.0` protocol:
     1. Build the peer binary: `go build -o bin/peer ./cmd/peer`
     2. Start **Peer A (Listener/Seeder)** and ingest a file: `./bin/peer -p 55555 -ingest test.mp4`
     3. Take note of Peer A's full multiaddress and the generated `ContentID` (e.g., `abcd12345...`).
     4. Open a second terminal window. Start **Peer B (Downloader)**, passing Peer A's address and fetching the content: 
        `CIPHER_CONFIG_DIR=/tmp/cipher-peer-b ./bin/peer -d /ip4/127.0.0.1/tcp/55555/p2p/12D3... -fetch abcd12345... -reassemble out.mp4`
     5. Observe Peer A's logs: It should log the ingestion and stream requests for manifest and chunks.
     6. Observe Peer B's logs: It should log resolving the manifest, downloading the chunks sequentially, and successfully reassembling the file.

   - **Relay Node Deployment**: To manually test the relay functionality (without full NAT traversal logic yet, just connection):
     1. Build the relay binary: `go build -o bin/relay ./cmd/relay`
     2. Start the relay: `./bin/relay`
     3. Ensure it outputs `Relay Service Started!` and prints its multiaddresses (e.g., `/ip4/127.0.0.1/tcp/4001/p2p/...`).
     4. To verify routing traffic, proceed to the **Relay Connectivity** step below.

   - **Relay Connectivity & Content Transfer**: To verify that a peer (e.g., Mac) can connect to another peer (e.g., Windows) through a public relay (e.g., Ubuntu server) and transfer content:
     1. Start your public relay node on the Ubuntu server and copy its multiaddress (e.g., `/ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>`).
     2. On **Peer A** (Listener, e.g., Windows), start the peer, provide the relay, and ingest a file:
        `./bin/peer -p 55555 -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -ingest test.mp4`
     3. Take note of Peer A's generated Relay Multiaddress and the returned `ContentID`.
     4. On **Peer B** (Dialer, e.g., Mac), start Peer B, provide the static relay, dial Peer A's relayed address, and fetch the content:
        `./bin/peer -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -d /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>/p2p-circuit/p2p/<PEER-A-ID> -fetch <ContentID> -reassemble out.mp4`
     5. **Verify Hole Punching**: After the initial relayed connection, both peers should log `[DCUtR] Hole Punch Event: StartHolePunch`. Following a successful attempt, you should see `EndHolePunch`, and the network connection will naturally upgrade to direct routing.
     6. **Verify Transfer Integrity**: Peer B will download all encrypted chunks sequentially, verifying their hashes, and finally reassemble them into `out.mp4`.

   - **Relay Fallback**: To verify that the system can still transfer data over the relay when direct connection fails (or is disabled):
     1. Start **Peer A** with the `-force-relay` flag to disable hole punching:
        `./bin/peer -p 55555 -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -force-relay -ingest test.mp4`
     2. Start **Peer B** with the `-force-relay` flag as well:
        `./bin/peer -relay /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID> -d /ip4/<PUBLIC-IP>/tcp/4001/p2p/<RELAY-ID>/p2p-circuit/p2p/<PEER-A-ID> -force-relay -fetch <ContentID> -reassemble out.mp4`
     3. Verify that the file transfers successfully. The logs should explicitly say `Path: Relay` instead of `Path: Direct`.

   - **Direct Upgrade (Hole Punching)**: To verify natural direct upgrade:
     1. Start Peer A and B with a relay, but without the `-force-relay` flag.
     2. Start a large transfer (e.g. 50-100MB).
     3. During the transfer, you should observe `[DCUtR] Hole Punch Event: StartHolePunch` and `EndHolePunch`. 
     4. Note that for a single stream, the underlying connection used is decided when the stream is created. Any *new* stream after successful hole punching will use the direct connection (you will see `Path: Direct` in the transfer logs). For large transfers initiated before hole punch completes, they may finish over Relay depending on the exact libp2p stream multiplexer routing. Wait for `EndHolePunch` to finish and initiate another transfer to verify it says `Path: Direct` and is much faster.

    - **Integrity Verification**: 
     1. This is automatically tested on every block fetched by the client via the verify-then-store mechanism.
     2. If a chunk gets corrupted, the client drops the transfer with a hash mismatch error before the chunk touches the disk.

## Transport Layer Test Matrix

Before moving to the Content Engine Foundation (Milestone 7), the transport layer is considered validated only after all the following manual tests pass:

- [☑️] **Small Payload**: 1 KB file transfer
- [☑️] **Medium Payload**: 1 MB file transfer
- [☑️] **Large Payload**: 50–100 MB file transfer
- [☑️] **Binary Format**: e.g., ZIP or JPEG file
- [☑️] **Bidirectional**: Transfer A→B and B→A
- [☑️] **Relay Fallback**: Transfer over relay only (disable DCUtR / hole punching)
- [☑️] **Direct Upgrade**: Transfer naturally switches to direct path after successful DCUtR
- [☑️] **Integrity**: SHA-256 verification succeeds for every single transfer

## Content Engine Test Matrix (Milestone 7)

The Content Engine Foundation completely decouples the filesystem from the transport layer. It is tested strictly via local automation before any P2P network integration.

To run the Content Engine test suite:
```bash
go test -v ./internal/content/...
```

The engine is considered validated only after all the following automated tests pass:

- [☑️] **Chunking**: Streams are correctly split into precisely sized chunks based on dynamic configuration.
- [☑️] **Encryption (XChaCha20-Poly1305)**: Every chunk is independently encrypted in-place using a 192-bit nonce, modifying the `CipherSize` correctly.
- [☑️] **Integrity & Hashing**: `Digest` (SHA-256) correctly hashes ciphertexts to yield `ChunkID`s, and securely verifies them before decryption.
- [☑️] **Decoupled Storage**: The `ChunkSource` and `ChunkSink` interfaces successfully store and retrieve chunks from the filesystem using content-addressed filenames.
- [☑️] **End-to-End Reassembly**: A large data stream is successfully ingested, chunked, encrypted, hashed, stored, retrieved, decrypted, and accurately reassembled back into its original sequence using the immutable `Manifest`.
- [☑️] **Corruption Handling**: Altering a stored chunk reliably triggers a `hash mismatch` or decryption failure upon reassembly.

### Robustness Tests (Advanced Validation)

While the above matrix represents the minimum acceptance criteria for the Content Engine, the following robustness tests validate its readiness for a decentralized network environment:

- [☑️] **Boundary Testing**: File sizes precisely hitting chunk boundaries (e.g., 0B, 1B, 31KB, 32KB, 32KB+1B, 1MB, 100MB).
- [ ] **Randomized Inputs**: End-to-end ingest and reassembly of randomly generated binary files (not just text) with verification via SHA-256.
- [ ] **Out-of-Order Reconstruction**: Reassembling a stream from chunks requested and returned in a completely random sequence.
- [ ] **Duplicate Chunk Handling**: Simulating swarming by providing duplicate chunks and verifying no corruption or duplicate writes occur.
- [☑️] **Missing Chunk Handling**: Explicitly deleting a chunk and asserting the engine cleanly returns `ErrMissingChunk` rather than panicking or producing partial output.
- [☑️] **Cryptographic Rejection**: Providing the wrong decryption key and ensuring the engine fails securely.
- [ ] **Manifest Validation**: Rejecting invalid manifests (duplicate IDs, invalid offsets, malformed JSON, etc) before any processing begins.
- [ ] **Determinism**: Asserting that ingesting the exact same file twice with the same key produces the expected object behavior (independent ciphertexts due to nonces).
- [ ] **Performance & Streaming**: Ingesting a 500MB file and asserting that memory remains strictly bounded (no full-file buffering).
- [ ] **API Contracts**: Running identical test suites against any future `ChunkSource` (e.g. Memory, IPFS, S3) to ensure seamless swapping.
- [☑️] **The 1000-Iteration Gauntlet**: Running 1000 loops of: Generate random file -> Random chunk size -> Random key -> Ingest -> Shuffle chunks -> Restore -> Compare SHA-256.

### Manual Testing (Content Engine CLI)

To manually test the pipeline, you can use the `content-test` CLI utility. As development progresses, this tool supports extensive commands for debugging and manual validation.

1. **Build the Test CLI**:
   ```bash
   go build -o bin/content-test ./cmd/content-test
   ```

2. **Ingest a File**:
   Create a sample file and run the ingest command:
   ```bash
   mkdir -p test_files
   echo "This is a test file for the content engine." > test_files/sample.txt
   ./bin/content-test -ingest test_files/sample.txt
   ```
   *Expected Output*: The CLI will log the total chunks created and save `test_files/manifest.json`. Check the `./test_files/content_store/` directory to see the stored encrypted chunks sharded by their SHA-256 hashes.

3. **Reassemble the File**:
   Using only the generated manifest and the chunks in the local store, reassemble the original file:
   ```bash
   ./bin/content-test -out test_files/restored.txt
   ```
   *Expected Output*: The `restored.txt` file will be created and its contents should perfectly match `sample.txt`.

4. **Future CLI Commands (Planned)**:
   For debugging future distributed networks, the CLI will support advanced validation:
   ```bash
   ./bin/content-test verify test_manifest.json
   ./bin/content-test inspect test_manifest.json
   ./bin/content-test list
   ./bin/content-test corrupt <chunkID>
   ./bin/content-test delete <chunkID>
   ```

## Continuous Integration
Tests are intended to be executed automatically upon pull requests via standard CI pipelines to maintain code quality across iterations.
