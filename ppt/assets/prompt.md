Yes. What you need is a **master prompt** that you can give to Claude/Codex/Gemini so the model understands **CIPHER, the intended architecture, the presentation narrative, the technical depth, and the design rules** rather than independently redesigning your PPT.

Below is a reusable prompt. I have deliberately made it **strict about not inventing architecture**, while allowing the model to improve visual presentation.

```text
MASTER PROMPT — CIPHER TECHNICAL PROJECT PRESENTATION
=====================================================

ROLE
====

You are a senior systems architect, distributed-systems engineer,
networking engineer, technical presentation designer, and scientific
communication expert.

You are helping create the final technical project presentation for
a project called CIPHER.

Your task is NOT to invent a different project.

Your task is to transform the existing CIPHER architecture,
implementation status, design decisions, and technical concepts into
a professional technical presentation.

The presentation must be understandable to:
- a professor
- a systems researcher
- a networking engineer
- a distributed-systems engineer
- a software engineer
- a student evaluating the project

The presentation must communicate:
- what problem CIPHER solves
- why the problem matters
- what the system does
- how the system is architected
- why the architecture was chosen
- how the network operates
- how content is prepared
- how providers are discovered
- how shards are transferred
- how failures are handled
- what has actually been implemented
- what remains future work

Do not turn the presentation into a marketing pitch.

Do not make unsupported claims.

Do not invent implementation details.

Do not claim something is implemented if it is only planned.

Do not confuse the control plane with the data plane.

Do not confuse Kademlia metadata with actual content storage.

Do not confuse Reed-Solomon fault tolerance with network fault tolerance.

Do not present blockchain/incentive functionality as complete unless the
provided project material explicitly says that it is implemented.

=====================================================
1. PROJECT IDENTITY
=====================================================

Project name:

CIPHER

Primary concept:

Decentralized content distribution over P2P networks.

Core idea:

A large file is transformed into distributed content that can be
published, discovered, transferred, verified, and recovered through
multiple independent peers.

CIPHER is not simply a file-transfer application.

It combines:

- content preparation
- distributed placement
- provider discovery
- decentralized metadata management
- P2P networking
- parallel content transfer
- cryptographic verification
- failure recovery

The system has two major conceptual planes:

CONTROL PLANE

and

DATA PLANE.

The control plane answers:

"Where is the content?"

The data plane answers:

"Can I actually retrieve the content?"

This distinction is fundamental.

=====================================================
2. HIGH-LEVEL SYSTEM MODEL
=====================================================

The primary actors are:

1. Publisher
2. Control Plane
3. Provider
4. Client
5. Network

Publisher:

The Publisher prepares the original content.

The Publisher:
- splits the file
- generates chunks
- applies Reed-Solomon encoding
- generates the manifest
- generates integrity information
- determines placement
- publishes metadata
- uploads or distributes assigned shards

Control Plane:

The Control Plane provides decentralized discovery and metadata
coordination.

It uses Kademlia DHT as the underlying distributed discovery mechanism.

The Control Plane can contain:
- provider registry
- manifest registry
- placement registry
- provider discovery
- shard-provider lookup

The Control Plane should not be presented as storing the actual large
file payload.

Provider:

A Provider stores assigned shards and serves them to clients.

Provider responsibilities include:
- provider registration
- provider state/heartbeat
- shard storage
- shard retrieval
- serving shard requests
- participating in P2P networking

Client:

The Client:
- resolves the manifest
- resolves placement information
- builds the chunk/shard-to-provider map
- establishes network connections
- requests shards
- verifies received content
- stores verified shards
- reconstructs the original file
- recovers from provider/network failures

Network:

The Network provides the actual communication layer.

Current concepts include:
- libp2p
- peer identities
- Relay / Circuit v2
- DCUtR hole punching
- direct peer connections
- multiplexed streams
- chunk protocol

=====================================================
3. CORE PRESENTATION NARRATIVE
=====================================================

The presentation must follow this conceptual progression:

WHAT PROBLEM ARE WE SOLVING?

↓

WHAT IS THE CORE IDEA?

↓

WHAT DOES CIPHER DO?

↓

HOW IS CIPHER ARCHITECTED?

↓

HOW IS CONTENT PREPARED?

↓

HOW DOES KAD EMLIA HELP DISCOVERY?

↓

HOW ARE SHARDS PLACED?

↓

HOW DOES THE P2P NETWORK CONNECT PEERS?

↓

HOW DOES THE CHUNK PROTOCOL WORK?

↓

HOW DOES PARALLEL DOWNLOAD WORK?

↓

HOW IS CONTENT VERIFIED?

↓

HOW DOES THE SYSTEM RECOVER FROM FAILURES?

↓

WHAT HAS BEEN IMPLEMENTED?

↓

WHAT COMES NEXT?

The presentation must feel like a story.

Each slide must prepare the audience for the next slide.

Do not jump directly into implementation details.

Do not introduce Kademlia before explaining why discovery is needed.

Do not introduce DCUtR before explaining why peers need connectivity.

Do not introduce Reed-Solomon before explaining why distributed content
needs recovery.

=====================================================
4. PRESENTATION STYLE
=====================================================

The presentation should look like a serious technical systems project.

Visual style:

- modern
- minimal
- technical
- clean
- high contrast
- professional
- engineering-oriented
- research/project-demo quality

Avoid:

- excessive gradients
- excessive decorative elements
- stock illustrations
- generic AI-generated technology backgrounds
- unnecessary 3D graphics
- excessive icons
- excessive text
- marketing slogans
- fake metrics
- unsupported performance claims

Prefer:

- architecture diagrams
- system flows
- protocol diagrams
- state transitions
- simple node graphs
- data-flow diagrams
- concise labels
- clear arrows
- consistent terminology
- consistent visual hierarchy

Every diagram should have a reason to exist.

Do not add a diagram merely to fill space.

=====================================================
5. SLIDE COUNT
=====================================================

Target approximately 18 slides.

Use the following slide structure.

Slide 1:
CIPHER

Slide 2:
Problem Statement

Slide 3:
Design Goals

Slide 4:
System Overview

Slide 5:
Core Features

Slide 6:
Architecture

Slide 7:
Content Preparation

Slide 8:
Kademlia DHT

Slide 9:
Shard Placement

Slide 10:
P2P Network

Slide 11:
Chunk Protocol

Slide 12:
Parallel Download

Slide 13:
Security & Integrity

Slide 14:
Download Flow

Slide 15:
CIPHER in Action

Slide 16:
Failure Recovery

Slide 17:
Future Implementations

Slide 18:
Team

Do not arbitrarily change this structure unless there is a strong
presentation-design reason.

=====================================================
6. SLIDE 1 — TITLE
=====================================================

Heading:

CIPHER

Subheading:

Decentralized Content Distribution over P2P Networks

The title slide should be minimal.

Include:
- project name
- subtitle
- team
- institution
- project/course information if provided

Do not put architecture details on the title slide.

Do not put long descriptions.

=====================================================
7. SLIDE 2 — PROBLEM STATEMENT
=====================================================

Heading:

Problem Statement

Subheading:

The Challenge of Reliable Distributed File Transfer

Explain the problem before explaining CIPHER.

The audience should understand:

Traditional centralized distribution has:
- centralized infrastructure
- server bandwidth bottlenecks
- single-source dependency
- failure concentration
- scalability challenges

Then introduce the distributed setting.

A distributed network introduces new problems:
- peers may fail
- peers may disconnect
- peers may be unreliable
- peers may have different bandwidth
- peers may be behind NAT
- content may be distributed across multiple providers
- discovery becomes difficult
- integrity must be verified
- recovery must happen without restarting the entire download

Do not claim that centralized systems are always bad.

Frame the problem precisely:

How can content be distributed across independent providers while
remaining discoverable, verifiable, transferable, and recoverable?

=====================================================
8. SLIDE 3 — DESIGN GOALS
=====================================================

Heading:

Design Goals

Subheading:

Decentralization • Reliability • Integrity • Efficient Transfer

Use four or five major goals.

Goal 1:

Decentralization

Content should not depend on a single provider.

Goal 2:

Reliability

Provider failures should not necessarily terminate the entire
download.

Goal 3:

Integrity

The client should cryptographically verify received content.

Goal 4:

Efficient Transfer

Multiple providers should be usable concurrently.

Goal 5:

Network Resilience

Peers should be able to communicate despite NAT and changing network
conditions where the underlying libp2p mechanisms support it.

Keep this slide conceptual.

Do not put detailed implementation APIs here.

=====================================================
9. SLIDE 4 — SYSTEM OVERVIEW
=====================================================

Heading:

System Overview

Subheading:

From One File to Distributed Content

This slide should provide the audience with the mental model.

Do NOT use the complete architecture diagram here.

Use a simple conceptual transformation.

Show:

ONE FILE

↓

CHUNKS / SHARDS

↓

MULTIPLE PROVIDERS

↓

CLIENT

↓

RECONSTRUCTED FILE

The visual should communicate:

A single file becomes distributed content.

Different providers can store different shards.

The client discovers and retrieves the necessary shards.

The client verifies and reconstructs the content.

A possible conceptual diagram:

Publisher

↓

File

↓

Chunking + Encoding

↓

Shard 0
Shard 1
Shard 2
Shard 3
...

↓

Provider A
Provider B
Provider C
Provider D

↓

Client

↓

Verified File

Use very little text.

The slide should answer:

"What is the big idea?"

Do not explain Kademlia here.

Do not explain Relay here.

Do not explain detailed protocol messages here.

=====================================================
10. SLIDE 5 — CORE FEATURES
=====================================================

Heading:

Core Features

Subheading:

Content Preparation • Discovery • Transfer • Recovery

This slide should introduce the four core capabilities.

Do not repeat the full architecture.

Use four connected conceptual stages.

Stage 1:

Content Preparation

Purpose:

Turn a file into distributed, verifiable content.

Key concepts:

- Chunking
- Reed-Solomon encoding
- Manifest
- Merkle root

Output:

Manifest + Shards

Stage 2:

Discovery

Purpose:

Determine where the content is available.

Key concepts:

- Kademlia DHT
- Provider discovery
- Placement map
- Shard-provider lookup

Output:

ChunkProviderMap

Stage 3:

Transfer

Purpose:

Retrieve content from multiple providers.

Key concepts:

- libp2p
- Relay/DCUtR
- parallel transfer
- cryptographic verification

Output:

Verified shards

Stage 4:

Recovery

Purpose:

Continue the download despite failures.

Key concepts:

- provider switching
- session resume
- missing-shard recovery
- Reed-Solomon reconstruction

Output:

Reconstructed file

The four stages should be visually connected:

Prepare

→

Discover

→

Transfer

→

Recover

At the bottom of the slide:

CIPHER transforms one file into distributed, discoverable,
verifiable, and recoverable content.

Do not make this slide too technical.

=====================================================
11. SLIDE 6 — ARCHITECTURE
=====================================================

Heading:

Architecture

Subheading:

Publisher • Control Plane • Provider • Client • Network

This is the detailed architecture slide.

Use the provided CIPHER architecture diagram if available.

Do not replace the project architecture with a generic architecture.

The architecture should clearly show:

Publisher

Control Plane

Kademlia DHT

Provider

Client

Network

The architecture should visually distinguish:

CONTROL PLANE

from

DATA PLANE.

Control Plane:

- provider registry
- manifest registry
- placement registry
- discovery
- DHT metadata

Data Plane:

- libp2p
- relay
- DCUtR
- direct peer connectivity
- chunk protocol
- actual shard transfer

The key conceptual statement should be:

Control Plane:
"Where is the data?"

Data Plane:
"Get the data."

The actual shard payload should remain associated with Providers,
not with the DHT.

=====================================================
12. SLIDE 7 — CONTENT PREPARATION
=====================================================

Heading:

Content Preparation

Subheading:

Chunking • Reed-Solomon Encoding • Manifest • Merkle Root

This slide should zoom into the Publisher.

Show a pipeline:

File

↓

ChunkFile()

↓

Data Chunks

↓

EncodeReedSolomon()

↓

Data + Parity Shards

↓

Hash / ChunkID

↓

Merkle Tree

↓

Merkle Root

↓

Manifest

The slide should explain why each step exists.

Chunking:

Break a large file into manageable units.

Reed-Solomon:

Create redundancy so missing shards can potentially be reconstructed.

Manifest:

Describe the content and its structure.

Merkle Root:

Provide cryptographic integrity structure.

Do not claim Reed-Solomon guarantees recovery from arbitrary numbers of
failures without considering the configured data/parity parameters.

Explain that recoverability depends on the configured redundancy.

=====================================================
13. SLIDE 8 — KADEMLIA DHT
=====================================================

Heading:

Kademlia DHT

Subheading:

Decentralized Discovery and Metadata Management

Do not present Kademlia as a generic computer-science lecture.

Present the subset relevant to CIPHER.

Explain:

Every participating DHT node has:
- PeerID
- routing table
- k-buckets
- DHT records

CIPHER-level records can include:

ProviderRecord

ManifestRecord

PlacementRecord

ShardProviderRecord

Explain that the DHT is used to locate metadata.

The DHT is not the storage location for the actual shard payload.

Show:

Client

↓

FindShardProviders()

↓

Kademlia

↓

Provider A
Provider B
Provider C

↓

Client opens Data Plane connection.

Also explain:

Kademlia routing state is different from CIPHER application records.

Do not imply every DHT node stores every record.

Do not claim the DHT is a centralized database.

=====================================================
14. SLIDE 9 — SHARD PLACEMENT
=====================================================

Heading:

Shard Placement

Subheading:

Mapping Content Shards to Providers

Show:

Shard 0 → Provider A

Shard 1 → Provider C

Shard 2 → Provider B

Shard 3 → Provider A

Shard 4 → Provider D

Explain the relationship:

Manifest

↓

PlacementMap

↓

Provider assignments

The Publisher determines placement.

The Control Plane makes placement metadata discoverable.

The Client resolves placement metadata.

The Provider stores the actual shard.

Explain the distinction between:

PlacementMap

and

ChunkProviderMap.

PlacementMap:

Publisher/control-plane representation of shard placement.

ChunkProviderMap:

Client-side execution plan mapping chunks/shards to providers available
for retrieval.

Do not conflate these two concepts.

=====================================================
15. SLIDE 10 — P2P NETWORK
=====================================================

Heading:

P2P Network

Subheading:

libp2p • Relay • DCUtR • Direct Connectivity

Explain the network layer.

The network stack includes:

- persistent peer identity
- libp2p host
- transport
- multiplexed streams
- Relay / Circuit v2
- DCUtR
- direct connections where possible

Show:

Client

↓

libp2p

↓

Relay

↓

Provider

and then:

DCUtR

↓

Direct Connection

Explain that the Relay can provide connectivity assistance.

Explain that DCUtR can attempt to upgrade the path to direct peer-to-peer
connectivity.

Do not claim hole punching always succeeds.

Do not claim Relay is always used for the entire transfer.

Make clear that actual behavior depends on network topology and libp2p.

=====================================================
16. SLIDE 11 — CHUNK PROTOCOL
=====================================================

Heading:

Chunk Protocol

Subheading:

Request • Transfer • Acknowledgement

Show the protocol as a sequence diagram.

Client

↓

REQUEST_CHUNK

↓

Provider

↓

CHUNK

↓

Client

↓

ACK

Use the project's actual protocol name if provided:

/cipher/chunk/1.0.0

If manifest messages are included in the current implementation,
show them accurately.

Possible flow:

REQUEST_MANIFEST

↓

MANIFEST

↓

REQUEST_CHUNK

↓

CHUNK

↓

ACK

Do not invent messages.

Only show protocol messages supported by the project documentation or
provided source.

Explain that the chunk protocol should remain focused on network
communication and should not own high-level transfer scheduling.

=====================================================
17. SLIDE 12 — PARALLEL DOWNLOAD
=====================================================

Heading:

Parallel Download

Subheading:

Concurrent Shard Retrieval from Multiple Providers

Show:

Client

↓

ChunkProviderMap

↓

Scheduler

↓

Worker Pool

↓

Provider A
Provider B
Provider C
Provider D

Each provider serves different shards concurrently.

Explain:

TransferManager:

Owns lifecycle/session state.

Scheduler:

Determines work distribution.

Workers:

Perform concurrent chunk/shard retrieval.

Content Engine:

Verifies/stores the result.

Show that provider failure does not necessarily require restarting the
whole transfer.

Example:

Provider A

Shard 0 ✓

Shard 3 ✓

Provider B

Shard 1 ✓

Provider B fails.

Scheduler reassigns missing work.

Provider C

Shard 1 ✓

Do not claim the scheduler uses a specific algorithm unless the project
documentation says so.

=====================================================
18. SLIDE 13 — SECURITY AND INTEGRITY
=====================================================

Heading:

Security & Integrity

Subheading:

Encryption • Hashing • Merkle Verification

Explain the security pipeline.

Received data

↓

Frame validation

↓

Size validation

↓

Decoding/decryption as appropriate

↓

Hash verification

↓

Merkle verification

↓

Manifest consistency

↓

Storage

Make the distinction:

Encryption protects confidentiality.

Hashing verifies content integrity.

Merkle proofs establish membership in the committed content structure.

Peer identity establishes network identity.

Do not claim hashing authenticates a malicious peer.

Do not claim encryption alone proves content correctness.

Do not claim Merkle verification replaces all protocol validation.

=====================================================
19. SLIDE 14 — DOWNLOAD FLOW
=====================================================

Heading:

Download Flow

Subheading:

Manifest Resolution → Provider Discovery → Parallel Download

Show the client-side flow.

Client receives ManifestID.

↓

ResolveManifest()

↓

ResolvePlacementMap()

↓

BuildChunkProviderMap()

↓

ConnectProvider()

↓

RequestShard()

↓

VerifyShard()

↓

StoreShard()

↓

Reconstruct

The flow should show recovery branching.

If provider fails:

SwitchProvider()

and continue.

If a shard is corrupted:

Reject.

Request from another provider.

If the client crashes:

ResumeDownload(sessionID)

if supported by the implementation.

Do not promise recovery behavior that is not implemented.

Use labels such as:

"Implemented"

"Planned"

where appropriate.

=====================================================
20. SLIDE 15 — CIPHER IN ACTION
=====================================================

Heading:

CIPHER in Action

Subheading:

End-to-End File Distribution Demo

This slide should contain actual evidence.

Prefer:
- terminal output
- screenshots
- logs
- live metrics
- provider status
- manifest output
- placement output
- transfer progress

Show a realistic sequence.

Publisher:

File uploaded.

Manifest created.

Placement generated.

Providers:

Shard distribution.

Client:

Manifest resolved.

Providers discovered.

Connections established.

Shards downloaded.

Verification succeeds.

File reconstructed.

If direct connectivity is demonstrated, show it.

If Relay is demonstrated, show it.

If Kademlia is demonstrated, show it.

Do not create fake screenshots.

Do not create fake benchmark numbers.

=====================================================
21. SLIDE 16 — FAILURE RECOVERY
=====================================================

Heading:

Failure Recovery

Subheading:

Provider Failure → Provider Switching → Resume

This slide should demonstrate the system's resilience.

Show:

Client

↓

Provider A

Shard 0 ✓

Shard 3 ✓

Provider B

Shard 1 ✓

Provider B disconnects.

↓

Scheduler detects failure.

↓

Find alternative provider.

↓

Request missing shard.

↓

Verify.

↓

Continue.

Also show:

Client crash

↓

Restore session

↓

Identify completed shards

↓

Queue missing shards

↓

Continue

For Reed-Solomon:

Missing shards can potentially be reconstructed if enough valid shards
remain according to the configured redundancy.

Do not say:

"Reed-Solomon solves network failures."

Instead say:

"Reed-Solomon provides content redundancy, while the network/scheduler
provides transfer fault tolerance."

This distinction is important.

=====================================================
22. SLIDE 17 — FUTURE IMPLEMENTATIONS
=====================================================

Heading:

Future Implementations

Subheading:

Scalability • Optimization • Incentives

Clearly distinguish future work.

Potential future areas:

- advanced provider selection
- bandwidth-aware placement
- dynamic replication
- improved DHT record lifecycle
- large-scale swarm testing
- provider reputation
- incentive mechanism
- lottery/payment mechanism
- blockchain settlement
- more advanced scheduling
- adaptive resource management
- transport hardening

Do not label planned functionality as implemented.

Use a visual:

CURRENT

↓

NEXT

↓

FUTURE

If the incentive system is not currently implemented, place it here.

=====================================================
23. SLIDE 18 — TEAM
=====================================================

Heading:

Team

Subheading:

Building CIPHER

List team members and responsibilities.

Examples:

Networking / Transport

Content Engine

Control Plane / DHT

Client / Scheduler

Security / Robustness

Only use actual team assignments.

Do not invent responsibilities.

=====================================================
24. ARCHITECTURE TERMINOLOGY
=====================================================

Use these terms consistently.

Publisher:

The entity that prepares and publishes content.

Provider:

A peer that stores and serves shards.

Client:

The entity requesting and reconstructing content.

Control Plane:

The distributed metadata/discovery layer.

Data Plane:

The actual content-transfer layer.

Manifest:

Describes content structure and integrity metadata.

ManifestID:

Identifier used to resolve the manifest.

PlacementMap:

Maps shards/content pieces to provider assignments.

PlacementMapID:

Identifier for the placement map.

ProviderList:

Set/list of candidate providers.

ProviderMetadata:

Metadata describing a provider.

ProviderState:

Current provider status/state.

ChunkProviderMap:

Client-side mapping from required content pieces to candidate providers.

Shard:

A content unit resulting from chunking and/or erasure coding.

ChunkID:

Cryptographic identifier for a chunk where applicable.

ContentID:

Identifier for the overall content.

Kademlia:

Underlying distributed DHT/discovery mechanism.

Relay:

Connectivity assistance mechanism.

DCUtR:

Direct Connection Upgrade through Relay.

libp2p:

P2P networking framework.

TransferManager:

Owns transfer lifecycle/session state.

Scheduler:

Owns work distribution.

Worker:

Performs individual retrieval work.

Content Engine:

Handles chunking, encryption/decryption, verification, storage, and
reassembly depending on the current implementation.

=====================================================
25. TERMINOLOGY THAT MUST NOT BE CONFUSED
=====================================================

Do not confuse:

Control Plane

with

Data Plane.

Do not confuse:

Manifest

with

PlacementMap.

Do not confuse:

PlacementMap

with

ChunkProviderMap.

Do not confuse:

Chunk

with

Shard

if the architecture distinguishes them.

Do not confuse:

Provider discovery

with

actual provider connection.

Do not confuse:

Kademlia routing table

with

CIPHER application records.

Do not confuse:

Reed-Solomon redundancy

with

network fault tolerance.

Do not confuse:

cryptographic integrity

with

peer trust.

Do not confuse:

PeerID

with

Ethereum wallet identity.

Do not present the blockchain identity system as complete unless
implementation evidence exists.

=====================================================
26. CURRENT NETWORK IMPLEMENTATION
=====================================================

The current network layer is based on libp2p.

Relevant capabilities include:

- persistent identity
- libp2p host
- TCP
- QUIC
- Relay / Circuit v2
- DCUtR
- peer connections
- streams
- custom chunk protocol

The Relay node currently uses:

libp2p

and

Circuit v2 relay.

If the provided source shows:

TCP on port 4001

and

QUIC on port 4002

you may mention this only in an implementation/demo slide if useful.

Do not make port numbers central to the presentation.

The important architecture is:

Application

↓

Transfer Layer

↓

Protocol

↓

libp2p Transport

↓

Relay / Direct Connection

↓

Remote Peer

=====================================================
27. CURRENT CONTENT ENGINE
=====================================================

The Content Engine is conceptually separated from transport.

Relevant components include:

Chunker

Crypto

Verifier

Manifest

Storage

Engine

The Content Engine can:

- split files
- encrypt chunks
- hash chunks
- create content identifiers
- build manifests
- store content-addressed chunks
- reassemble content

The current architecture uses XChaCha20-Poly1305 for chunk encryption
where documented.

SHA-256 is used for chunk/content verification where documented.

Do not change these algorithms without explicit project instruction.

=====================================================
28. PERSISTENT SESSION MODEL
=====================================================

The transfer architecture supports client-side session state.

Conceptually:

TransferManager

↓

Session

↓

Completed shard/chunk state

↓

Scheduler

↓

Missing work

If the implementation persists session state, explain:

The client does not need to restart the entire transfer after a crash.

Only missing content needs to be scheduled again.

Do not claim server-side session state unless implemented.

=====================================================
29. FAILURE MODEL
=====================================================

CIPHER assumes that peers and networks can fail.

Examples:

Provider offline.

Provider timeout.

Network interruption.

NAT rebinding.

Relay failure.

Client crash.

Corrupted content.

Duplicate content.

Slow provider.

Unresponsive provider.

The architecture should recover where possible.

Use the principle:

FAILURE OF ONE PROVIDER

≠

FAILURE OF THE ENTIRE DOWNLOAD.

=====================================================
30. SECURITY MODEL
=====================================================

The client should not blindly trust a provider.

The client should trust cryptographic evidence and protocol validation.

Relevant checks may include:

- message size validation
- protocol validation
- chunk identity
- hash verification
- Merkle proof verification
- manifest consistency
- duplicate detection
- storage validation

If a hash does not match:

Reject the content.

Do not accept corrupted data.

If a provider goes offline:

Try another provider if available.

If multiple providers offer the same valid content:

The client can use the first correctly verified result depending on the
scheduler policy.

Do not claim that provider identity is irrelevant in all security
contexts.

Identity still matters for:
- connection management
- accountability
- rate limiting
- abuse tracking
- future incentives

But content acceptance should depend on cryptographic verification.

=====================================================
31. RESOURCE PROTECTION
=====================================================

The project has identified public-network threats.

Potential threats:

- oversized messages
- stream floods
- connection floods
- slow peers
- memory exhaustion
- worker starvation
- excessive concurrent transfers
- malicious protocol messages

The architecture should eventually include:

- ResourceManager
- ConnectionManager
- ConnectionGater
- adaptive stream timeouts
- limited readers
- message-size limits
- bounded worker pools
- per-peer limits
- rate limiting
- cleanup

Do not claim these are implemented unless source material confirms them.

If they are future work, put them in:

Future Implementations

or

Security Roadmap.

=====================================================
32. RELAY ARCHITECTURE
=====================================================

Explain Relay as connectivity infrastructure.

A Relay may help two peers communicate when direct connectivity is
initially unavailable.

Conceptual flow:

Peer A

↓

Relay

↓

Peer B

Then:

DCUtR

↓

Direct Connection

The Relay should not be described as the permanent central file server.

The project direction is P2P transfer with relay-assisted connectivity.

Do not describe Render as a fundamental architectural dependency unless
the current deployment actually requires it.

Render is an infrastructure deployment choice, not the conceptual
architecture.

=====================================================
33. KADEMLIA SCOPE
=====================================================

Do not turn the presentation into a generic Kademlia tutorial.

CIPHER only needs the Kademlia capabilities required for the Control
Plane.

Relevant functionality:

Provider discovery.

Manifest resolution.

Placement resolution.

Shard-provider lookup.

Provider state.

Do not present unnecessary Kademlia functionality as a project feature.

Do not imply that the project implements every Kademlia feature from
scratch.

If go-libp2p Kademlia is used, distinguish:

Kademlia implementation provided by the library

from

CIPHER application-level records and contracts.

=====================================================
34. CONTROL PLANE SCOPE
=====================================================

The Control Plane is a cross-layer coordination mechanism.

It should coordinate:

- provider discovery
- manifest resolution
- placement resolution
- shard-provider discovery
- provider state

It should not directly perform the entire data transfer.

Correct conceptual separation:

Control Plane:

"Who has the content?"

Data Plane:

"Transfer the content."

Transfer Manager:

"Coordinate the download."

Scheduler:

"Assign work."

Content Engine:

"Validate/store/reconstruct content."

=====================================================
35. FUNCTION NAMING
=====================================================

If function names are shown, use the project's naming system.

Publisher:

ChunkFile()

EncodeReedSolomon()

BuildManifest()

GenerateMerkleRoot()

RequestProviders()

AssignChunksToProviders()

PublishManifest()

PublishPlacementMap()

UploadAssignedShards()

Control Plane:

RegisterProvider()

UpdateProviderState()

QueryAvailableProviders()

RemoveProvider()

RegisterManifest()

ResolveManifest()

RegisterPlacementMap()

ResolvePlacementMap()

FindProviders()

FindShardProviders()

Provider:

Register()

Heartbeat()

StoreShard()

ReadShard()

HandleShardRequest()

Client:

ResolveManifest()

ResolvePlacementMap()

BuildChunkProviderMap()

StartParallelDownload()

RequestShard()

VerifyShard()

StoreShard()

ResumeDownload()

SwitchProvider()

Network:

ConnectProvider()

EstablishDirectConnection()

OpenChunkStream()

SendProtocolMessage()

ReceiveProtocolMessage()

CloseChunkStream()

Do not create random alternative names.

=====================================================
36. DIAGRAM RULES
=====================================================

All architecture diagrams must:

- have a clear direction
- use consistent arrows
- use consistent names
- distinguish control plane from data plane
- avoid crossing lines where possible
- avoid unnecessary detail
- use grouping boundaries
- show inputs and outputs
- show actual system components

Every diagram must answer one question.

Examples:

System Overview:

"What is the overall idea?"

Architecture:

"How are components organized?"

Content Preparation:

"How does a file become shards?"

Kademlia:

"How is metadata discovered?"

Network:

"How do peers connect?"

Chunk Protocol:

"How do peers exchange content?"

Parallel Download:

"How does the client use multiple providers?"

Failure Recovery:

"How does the system continue after failure?"

Do not put all diagrams into one slide.

=====================================================
37. VISUAL HIERARCHY
=====================================================

Every slide should have:

1. Slide title
2. Short subtitle
3. Main visual
4. Small supporting explanation

Avoid:

large paragraphs.

Prefer:

3–5 bullets maximum.

Prefer:

one major diagram.

Prefer:

one central message.

Use emphasis for:

important terms

component names

technical mechanisms

Use consistent visual treatment for:

Publisher

Control Plane

Provider

Client

Network

=====================================================
38. TEXT DENSITY
=====================================================

Do not fill slides with documentation.

The PPT is not the architecture document.

The PPT is not the implementation specification.

The PPT is not the API reference.

The PPT is a visual explanation of the project.

Detailed information belongs in:

- documentation
- README
- architecture.md
- protocol specification
- testing documentation

The presentation should show only what the audience needs to understand
the system.

=====================================================
39. TECHNICAL DEPTH
=====================================================

The presentation should be technically serious.

Do not simplify to the point of being inaccurate.

Use correct terms:

distributed systems

content addressing

erasure coding

Merkle tree

DHT

Kademlia

NAT traversal

hole punching

multiplexing

peer identity

resource management

stream lifecycle

session persistence

parallel scheduling

backpressure

cryptographic verification

Do not use vague phrases such as:

"AI-powered"

"next-generation"

"revolutionary"

"seamless"

"unbreakable"

unless explicitly supported.

=====================================================
40. IMPLEMENTATION STATUS
=====================================================

Always distinguish:

IMPLEMENTED

TESTED

IN PROGRESS

PLANNED

FUTURE

If the source material says a component is implemented:

present it as implemented.

If the source material says it is under development:

present it as ongoing.

If the source material only discusses it:

present it as planned/research.

Never convert a design discussion into an implementation claim.

=====================================================
41. RESEARCH CLAIMS
=====================================================

If external research is required:

Use authoritative sources where possible.

Relevant sources may include:

libp2p documentation

go-libp2p documentation

Kademlia literature

RFCs

BitTorrent technical literature

IPFS documentation

Filecoin architecture

Syncthing architecture

QUIC RFC 9000

Go networking documentation

OWASP DoS guidance

Academic distributed-systems literature

If a claim comes from external research, distinguish it from CIPHER's
actual implementation.

Do not claim:

"CIPHER uses X because research proves X is best"

unless evidence supports that conclusion.

=====================================================
42. DEMO PRINCIPLES
=====================================================

The demo should show actual system behavior.

A good demo sequence:

1. Start providers.
2. Start Control Plane/DHT.
3. Start Publisher.
4. Prepare content.
5. Generate manifest.
6. Generate placement.
7. Register/discover providers.
8. Start Client.
9. Resolve manifest.
10. Resolve placement.
11. Build provider map.
12. Establish peer connections.
13. Download shards.
14. Verify shards.
15. Reconstruct content.
16. Demonstrate provider failure if reliable.
17. Show recovery.

Do not fake failures.

Do not fake metrics.

Do not fake DHT records.

=====================================================
43. PERFORMANCE CLAIMS
=====================================================

Do not invent:

throughput

latency

speedup

scalability

provider count

failure recovery time

bandwidth efficiency

CPU utilization

memory usage

If benchmark data exists, show the actual measured values.

If no benchmark exists, write:

"Performance evaluation pending."

Do not write:

"10x faster"

without measured evidence.

=====================================================
44. FAILURE DEMO
=====================================================

If demonstrating provider failure:

Initial:

Provider A
Provider B
Provider C

Client downloads from all.

Then:

Provider B disconnects.

Client should:

detect failure

identify missing work

select another provider

resume request

verify content

continue reconstruction

The audience should see:

"One peer failed."

not:

"The whole network failed."

=====================================================
45. REED-SOLOMON EXPLANATION
=====================================================

Explain Reed-Solomon precisely.

Suppose:

Data shards = K

Parity shards = M

Total shards = K + M

The file can be reconstructed if sufficient valid shards remain according
to the configured erasure-coding parameters.

Do not say:

"Any number of providers can fail."

Do not say:

"Reed-Solomon solves network failures."

Correct distinction:

Reed-Solomon provides redundancy at the content layer.

Scheduler/provider switching provides recovery at the network-transfer
layer.

Together they improve resilience.

=====================================================
46. CONTROL PLANE VS DATA PLANE
=====================================================

This is one of the most important architectural concepts.

Control Plane:

Provider metadata.

Manifest metadata.

Placement metadata.

Discovery.

Lookup.

State.

Data Plane:

Actual shard payload.

P2P connections.

Chunk streams.

Transport.

Verification.

Storage.

The PPT should visually reinforce this distinction.

Use:

CONTROL PLANE

"Where?"

DATA PLANE

"Get it."

=====================================================
47. PROVIDER TRUST MODEL
=====================================================

CIPHER operates in an environment where providers may be unreliable.

Do not assume:

Provider is honest.

Provider stays online.

Provider has correct data.

Provider has sufficient bandwidth.

Provider finishes requests.

The client should rely on:

cryptographic verification

protocol validation

timeouts

resource limits

alternative providers

recovery mechanisms

The presentation should communicate:

"Proof over trust."

But do not oversimplify.

Network identity still has operational value.

=====================================================
48. CLIENT-FIRST DESIGN
=====================================================

The Client is the primary beneficiary of reliability.

The client should not have to:

restart the entire download because one provider failed.

trust a provider blindly.

know the entire network topology.

manually choose every provider.

manually reconnect after every network interruption.

The architecture should automate:

discovery

provider selection

parallel transfer

verification

retry

recovery

resume

where supported by implementation.

=====================================================
49. PROVIDER RESPONSIBILITIES
=====================================================

Provider responsibilities:

Register.

Maintain state.

Store assigned shards.

Serve shard requests.

Maintain network connectivity.

Respond within protocol limits.

Do not put all Provider responsibilities on the Control Plane.

The Provider owns actual content storage.

=====================================================
50. PUBLISHER RESPONSIBILITIES
=====================================================

Publisher responsibilities:

Prepare content.

Generate shards.

Generate manifest.

Generate integrity metadata.

Determine placement.

Publish metadata.

Distribute content.

Do not make the Publisher responsible for client-side download
scheduling.

=====================================================
51. CLIENT RESPONSIBILITIES
=====================================================

Client responsibilities:

Resolve content.

Resolve placement.

Discover providers.

Build provider map.

Schedule downloads.

Verify content.

Store valid shards.

Recover failures.

Reconstruct content.

The Client is not required to trust providers.

=====================================================
52. NETWORK RESPONSIBILITIES
=====================================================

Network layer responsibilities:

Peer identity.

Connection establishment.

Transport.

Relay.

NAT traversal.

Stream creation.

Message transmission.

Stream cleanup.

The network layer should not own:

placement policy

content reconstruction

high-level download scheduling

manifest semantics

provider incentives

=====================================================
53. SCHEDULER RESPONSIBILITIES
=====================================================

Scheduler responsibilities:

Maintain pending work.

Assign chunks/shards to workers.

Coordinate concurrent requests.

Handle failed work.

Retry where policy permits.

Switch providers.

Respect resource limits.

Do not claim a specific scheduling algorithm unless implemented.

Possible future algorithms:

rarest-first

bandwidth-aware

latency-aware

provider-scoring

availability-aware

But label these future work unless implemented.

=====================================================
54. TRANSFER MANAGER RESPONSIBILITIES
=====================================================

TransferManager owns:

transfer lifecycle

session state

progress

retry policy

resume

failure coordination

It should coordinate the Scheduler rather than duplicate its work.

=====================================================
55. CONTENT ENGINE RESPONSIBILITIES
=====================================================

Content Engine owns:

content preparation

chunking

encryption/decryption where applicable

hashing

verification

manifest construction

storage

reassembly

Keep it decoupled from the transport layer.

=====================================================
56. PROTOCOL RESPONSIBILITIES
=====================================================

Protocol layer owns:

message definitions

encoding/decoding

validation

protocol version

request/response semantics

message ordering

size constraints

It should not own:

global scheduling

DHT provider selection

file reconstruction

provider reputation

=====================================================
57. SECURITY ROADMAP
=====================================================

If security hardening is presented, discuss:

Resource Manager.

Connection Manager.

Connection Gater.

Stream timeout.

Memory limits.

Message size limits.

Rate limiting.

Per-peer concurrency.

Worker limits.

Connection limits.

Relay limits.

Fuzz testing.

Slow-peer testing.

Flood testing.

OOM testing.

Do not claim all of these are already implemented.

=====================================================
58. PROTOCOL HARDENING
=====================================================

Potential protections:

maximum message size

safe decoding

request validation

protocol ordering

stream timeout

context cancellation

bounded buffers

limited readers

duplicate detection

invalid-message rejection

Do not allocate memory based on untrusted remote lengths without bounds.

Do not show implementation details unless supported by source.

=====================================================
59. OBSERVABILITY
=====================================================

If observability is included:

Show:

structured logs

connection statistics

transfer progress

provider status

download failures

retry counts

scheduler state

DHT lookup events

transport events

Metrics should only be shown if actually available.

Do not invent dashboards.

=====================================================
60. SLIDE TRANSITIONS
=====================================================

Use conceptual transitions.

Slide 4:

One file becomes distributed content.

Slide 5:

That process has four capabilities.

Slide 6:

Those capabilities map onto the system architecture.

Slide 7:

Zoom into content preparation.

Slide 8:

Zoom into discovery.

Slide 9:

Zoom into placement.

Slide 10:

Zoom into networking.

Slide 11:

Zoom into protocol.

Slide 12:

Zoom into parallel transfer.

Slide 13:

Zoom into verification.

Slide 14:

Combine the components into client workflow.

Slide 15:

Show actual implementation.

Slide 16:

Show failure behavior.

Slide 17:

Show future work.

=====================================================
61. SLIDE 4 DESIGN REQUIREMENT
=====================================================

Slide 4 must NOT be a duplicate of Slide 6.

Slide 4 should be conceptual.

Recommended visual:

ONE FILE

↓

DISTRIBUTED SHARDS

↓

MULTIPLE PROVIDERS

↓

CLIENT

↓

RECONSTRUCTED FILE

The audience should understand the fundamental idea without knowing
Kademlia or libp2p.

=====================================================
62. SLIDE 5 DESIGN REQUIREMENT
=====================================================

Slide 5 should introduce:

Prepare

Discover

Transfer

Recover

Each should have:

one purpose

2–4 technical concepts

one output

Do not make it another architecture diagram.

The slide should communicate the CIPHER lifecycle.

=====================================================
63. SLIDE 6 DESIGN REQUIREMENT
=====================================================

Slide 6 should use the detailed architecture.

The audience should now understand:

what the file becomes

what discovery means

what transfer means

what recovery means

Only now should they see:

Publisher

Control Plane

Kademlia

Providers

Client

Network

=====================================================
64. CONTENT PREPARATION SLIDE
=====================================================

Slide 7 should be the first deep technical slide.

Show:

File

↓

Chunker

↓

Chunks

↓

Reed-Solomon

↓

Data + Parity Shards

↓

Hashing

↓

Merkle Tree

↓

Manifest

Do not explain all cryptographic details here.

Keep them for Security & Integrity.

=====================================================
65. KADEMLIA SLIDE
=====================================================

Slide 8 should explain:

What information is discovered?

Who participates?

What does a DHT node maintain?

How does the client find providers?

Show a conceptual DHT graph.

Do not turn the slide into a Kademlia algorithm tutorial.

=====================================================
66. PLACEMENT SLIDE
=====================================================

Slide 9 should answer:

"Which provider stores which shard?"

Show:

PlacementMap

and

Provider assignments.

Do not confuse placement with transfer.

=====================================================
67. NETWORK SLIDE
=====================================================

Slide 10 should answer:

"How does the client connect to the provider?"

Show:

libp2p

Relay

DCUtR

Direct connection

Do not explain DHT here.

=====================================================
68. PROTOCOL SLIDE
=====================================================

Slide 11 should answer:

"What messages are exchanged?"

Show the request/response sequence.

Use actual protocol identifiers.

=====================================================
69. PARALLEL DOWNLOAD SLIDE
=====================================================

Slide 12 should answer:

"How does CIPHER use multiple providers?"

Show:

Client

↓

Scheduler

↓

Worker Pool

↓

Multiple Providers

Use a real example.

=====================================================
70. SECURITY SLIDE
=====================================================

Slide 13 should answer:

"How does CIPHER know what it received is correct?"

Show:

Received data

↓

Validation

↓

Hash

↓

Merkle

↓

Manifest

↓

Store

Do not overload with every security mechanism.

=====================================================
71. DOWNLOAD FLOW SLIDE
=====================================================

Slide 14 should combine:

Resolve Manifest

↓

Resolve Placement

↓

Build Provider Map

↓

Connect

↓

Download

↓

Verify

↓

Recover if needed

↓

Reconstruct

This is the end-to-end client flow.

=====================================================
72. DEMO SLIDE
=====================================================

Slide 15 should prove implementation.

Do not merely repeat architecture.

Show actual output.

Use real logs.

Use real terminal screenshots.

Use real network events.

Use real transfer progress.

=====================================================
73. RECOVERY SLIDE
=====================================================

Slide 16 should show one failure and one recovery.

Do not list ten failures.

Pick the strongest demo:

Provider failure.

Show:

Provider B fails.

Scheduler detects it.

Provider C takes over.

Transfer continues.

This tells the story better.

=====================================================
74. FUTURE SLIDE
=====================================================

Slide 17 should separate:

Implemented

from

Future.

Possible future:

advanced placement

dynamic replication

provider reputation

incentives

blockchain settlement

large-scale testing

adaptive scheduling

transport hardening

=====================================================
75. TEAM SLIDE
=====================================================

Slide 18 should be simple.

Names.

Roles.

Contributions.

No technical diagram.

=====================================================
76. PRESENTATION SPEAKER NOTES
=====================================================

For every slide, generate speaker notes.

Speaker notes should explain:

What the audience should understand.

What the presenter should say.

What technical detail can be discussed if asked.

Do not put speaker notes onto the visual slide.

=====================================================
77. SPEAKER NOTE STYLE
=====================================================

Speaker notes should sound natural.

Do not write:

"This slide demonstrates an innovative paradigm."

Write:

"Here the main idea is that CIPHER does not treat the file as one
monolithic object. The Publisher prepares it into smaller pieces and
distributes those pieces across providers."

Keep speaker notes concise.

=====================================================
78. TECHNICAL QUESTIONS TO ANTICIPATE
=====================================================

Prepare answers for:

Why Kademlia?

Why Reed-Solomon?

Why libp2p?

Why Relay?

Why DCUtR?

Why SHA-256?

Why Merkle trees?

Why content addressing?

Why multiple providers?

What happens if a provider disappears?

What happens if a chunk is corrupted?

What happens if all providers for a shard disappear?

What happens if the client crashes?

What happens if the network changes?

What happens if the DHT node fails?

Does the DHT store the file?

How is placement determined?

How is provider trust handled?

How is duplicate content handled?

How is resource exhaustion prevented?

How does the scheduler choose providers?

What is the difference between control plane and data plane?

What is implemented today?

What is future work?

=====================================================
79. DO NOT OVERCLAIM
=====================================================

Never say:

"Fully decentralized"

unless the architecture and implementation justify the statement.

Never say:

"Zero downtime"

Never say:

"Guaranteed delivery"

Never say:

"Impossible to attack"

Never say:

"Always direct P2P"

Never say:

"All failures are recovered"

Never say:

"Reed-Solomon guarantees availability"

Never say:

"Kademlia stores the file"

Never say:

"Hash proves provider honesty"

Use precise language.

=====================================================
80. FINAL PRESENTATION QUALITY CHECK
=====================================================

Before finalizing the PPT, check:

Does Slide 4 explain the idea?

Does Slide 5 explain the four core capabilities?

Does Slide 6 explain the architecture?

Does Slide 7 explain content preparation?

Does Slide 8 explain Kademlia?

Does Slide 9 explain placement?

Does Slide 10 explain networking?

Does Slide 11 explain protocol communication?

Does Slide 12 explain parallel download?

Does Slide 13 explain integrity?

Does Slide 14 explain end-to-end download?

Does Slide 15 prove implementation?

Does Slide 16 prove recovery?

Does Slide 17 distinguish future work?

Does Slide 18 identify the team?

=====================================================
81. NO REPETITION RULE
=====================================================

Do not explain the same concept in multiple slides.

For example:

Do not explain Kademlia in Slide 4.

Do not explain Kademlia in Slide 5.

Explain it deeply only in Slide 8.

Slide 4 only establishes the concept of discovery.

Slide 5 names discovery.

Slide 8 explains Kademlia.

Similarly:

Slide 4 introduces distributed content.

Slide 5 introduces transfer.

Slide 10 explains network connectivity.

Slide 11 explains protocol messages.

Slide 12 explains parallel transfer.

=====================================================
82. INFORMATION DENSITY RULE
=====================================================

One slide:

One primary question.

One main visual.

One main takeaway.

Maximum approximately:

5 bullets

or

one diagram

or

one table.

Do not use all three unless necessary.

=====================================================
83. COLOR SYSTEM
=====================================================

Use a consistent semantic color system if a visual theme is selected.

Suggested semantic categories:

Publisher:

one consistent color.

Control Plane:

another consistent color.

Provider:

another consistent color.

Client:

another consistent color.

Network:

neutral.

Do not use colors randomly.

Use color to reinforce architecture.

If the source presentation already has a color system, preserve it.

=====================================================
84. TYPOGRAPHY
=====================================================

Use:

large slide headings

short subtitles

large labels

minimal body text

Readable diagrams.

Do not use tiny text merely to fit more information.

If a diagram requires tiny text:

simplify the diagram.

=====================================================
85. DIAGRAM QUALITY
=====================================================

Every arrow should mean something.

Use directional arrows consistently.

Do not use decorative arrows.

Use:

solid arrows

for actual flow.

Use:

dashed arrows

for metadata/control relationships.

Use:

different boundary styles

for control plane/data plane if appropriate.

Label important interfaces.

=====================================================
86. ARCHITECTURE DIAGRAM RULE
=====================================================

Do not redraw the architecture in a fundamentally different way from
the project's documented architecture.

You may improve:

spacing

alignment

typography

visual grouping

labels

arrow clarity

You may not change:

responsibilities

component boundaries

data ownership

control-plane/data-plane distinction

without explicit instruction.

=====================================================
87. SOURCE FIDELITY
=====================================================

If a provided document or image says something:

preserve its terminology.

If it shows a component:

do not remove it unless asked.

If the source is ambiguous:

do not silently invent an implementation.

Flag the ambiguity.

=====================================================
88. USER'S CURRENT ARCHITECTURE
=====================================================

Use this conceptual architecture as the baseline:

Publisher

↓

Control Plane / Kademlia

↓

Provider discovery

↓

Placement

↓

Client

↓

Network

↓

Providers

The Provider stores actual shards.

The Client retrieves them.

The Control Plane provides metadata/discovery.

The Network carries data.

=====================================================
89. CIPHER CORE FUNCTIONALITY
=====================================================

Use this exact four-stage model:

CONTENT PREPARATION

DISCOVERY

TRANSFER

RECOVERY

The corresponding concepts are:

CONTENT PREPARATION:

Chunking

Reed-Solomon

Manifest

Merkle Root

DISCOVERY:

Kademlia

Provider Discovery

Placement

Shard Provider Lookup

TRANSFER:

libp2p

Relay

DCUtR

Parallel Download

Verification

RECOVERY:

Provider Switching

Session Resume

Retry

Reconstruction

=====================================================
90. FINAL SLIDE TITLES
=====================================================

Use these exact headings:

1. CIPHER

2. Problem Statement

3. Design Goals

4. System Overview

5. Core Features

6. Architecture

7. Content Preparation

8. Kademlia DHT

9. Shard Placement

10. P2P Network

11. Chunk Protocol

12. Parallel Download

13. Security & Integrity

14. Download Flow

15. CIPHER in Action

16. Failure Recovery

17. Future Implementations

18. Team

=====================================================
91. FINAL SUBTITLES
=====================================================

Slide 1:

Decentralized Content Distribution over P2P Networks

Slide 2:

The Challenge of Reliable Distributed File Transfer

Slide 3:

Decentralization • Reliability • Integrity • Efficient Transfer

Slide 4:

From One File to Distributed Content

Slide 5:

Content Preparation • Discovery • Transfer • Recovery

Slide 6:

Publisher • Control Plane • Provider • Client • Network

Slide 7:

Chunking • Reed-Solomon Encoding • Manifest • Merkle Root

Slide 8:

Decentralized Discovery and Metadata Management

Slide 9:

Mapping Content Shards to Providers

Slide 10:

libp2p • Relay • DCUtR • Direct Connectivity

Slide 11:

Request • Transfer • Acknowledgement

Slide 12:

Concurrent Shard Retrieval from Multiple Providers

Slide 13:

Encryption • Hashing • Merkle Verification

Slide 14:

Manifest Resolution → Provider Discovery → Parallel Download

Slide 15:

End-to-End File Distribution Demo

Slide 16:

Provider Failure → Provider Switching → Resume

Slide 17:

Scalability • Optimization • Incentives

Slide 18:

Building CIPHER

=====================================================
92. DESIGN OF SLIDE 4
=====================================================

Slide 4 should visually show:

ONE FILE

↓

DISTRIBUTED SHARDS

↓

MULTIPLE PROVIDERS

↓

CLIENT

↓

RECONSTRUCTED FILE

Do not show the entire control plane.

Do not show every component.

Do not show function names.

Do not show detailed Kademlia.

Do not show protocol messages.

The slide's purpose is:

"Explain the idea in 10 seconds."

=====================================================
93. DESIGN OF SLIDE 5
=====================================================

Slide 5 should visually show:

PREPARE

↓

DISCOVER

↓

TRANSFER

↓

RECOVER

Each stage gets:

short purpose

few mechanisms

output

The slide's purpose is:

"Explain what CIPHER actually does."

=====================================================
94. DESIGN OF SLIDE 6
=====================================================

Slide 6 should show the complete architecture.

The slide's purpose is:

"Explain where the functionality lives."

Use:

Publisher

Control Plane

Provider

Client

Network

Make Control Plane and Data Plane visibly different.

=====================================================
95. PRESENTATION NARRATIVE
=====================================================

The presenter should be able to say:

"CIPHER starts with a file."

"That file is transformed into distributed content."

"The system then needs to know where the content exists."

"That is where the Control Plane and Kademlia come in."

"Once the client knows which providers can serve the content,
the Data Plane establishes P2P connections."

"Multiple providers can serve content concurrently."

"Every received piece is verified."

"If a provider fails, the client does not necessarily restart."

"It can continue with another provider or use available redundancy."

"That is the central reliability story of CIPHER."

=====================================================
96. AVOID GENERIC CLOUD ARCHITECTURE
=====================================================

Do not make CIPHER look like:

Frontend

↓

API Gateway

↓

Microservices

↓

Database

↓

Cloud Storage

This is not the architecture.

CIPHER is:

Publisher

+

Control Plane

+

P2P Providers

+

Client

+

Network

=====================================================
97. AVOID GENERIC BLOCKCHAIN ARCHITECTURE
=====================================================

Do not put Ethereum at the center of the architecture unless the
current presentation explicitly focuses on the incentive layer.

The current core architecture is:

P2P content distribution.

Blockchain/incentives may be future work.

If discussed, show it as a future/settlement layer.

=====================================================
98. AVOID OVERLOADING KADEMLIA
=====================================================

Kademlia should not be presented as:

storage

scheduler

payment layer

transport

content engine

It is primarily:

distributed discovery / metadata routing substrate.

=====================================================
99. AVOID OVERLOADING PROVIDERS
=====================================================

Providers should not be shown as:

DHT

scheduler

publisher

client

relay

unless the actual architecture says that a single node can perform
multiple roles.

Keep role responsibilities clear.

=====================================================
100. FINAL INSTRUCTION
=====================================================

Create the presentation as if it will be evaluated by a technically
strong professor who will ask:

"Why is this component here?"

"What problem does it solve?"

"Why is this architecture better?"

"What happens when this component fails?"

"Where is the actual data?"

"Where is the metadata?"

"Who makes the decision?"

"Who transfers the data?"

"What is trusted?"

"What is cryptographically verified?"

"What is implemented?"

"What is future work?"

Every architectural element should have a defensible answer.

The presentation must be:

technically accurate

visually clear

architecturally faithful

implementation-aware

research-oriented

demo-oriented

concise

professional

Do not invent features.

Do not fabricate results.

Do not fabricate benchmarks.

Do not claim future work is implemented.

Do not replace CIPHER with a generic P2P architecture.

Use the provided CIPHER documentation, diagrams, code, and project
description as the source of truth.

When information is missing:

say that it is unspecified.

When something is planned:

label it planned.

When something is implemented:

label it implemented.

When something is experimentally demonstrated:

label it demonstrated/tested.

The final presentation should make the audience understand one central
idea:

CIPHER turns a file into distributed content, uses a decentralized
Control Plane to discover where that content exists, uses a P2P Data
Plane to retrieve it from multiple providers, verifies the content
cryptographically, and continues/reconstructs the transfer when peers
or network paths fail.

END OF MASTER PROMPT
```
