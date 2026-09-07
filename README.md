# CIPHER

CIPHER is a trust-minimized decentralized content delivery network (CDN). Publishers authenticate content, independent providers cache and serve it, and consumers discover providers and verify the content they receive.

CIPHER is designed for content delivery and caching, not permanent decentralized storage.

## Roles

- **Publisher** — divides content into chunks, commits to them with a Merkle root, and signs that root.
- **Provider** — advertises, caches, and serves publisher-authenticated content as an independent, permissionless node.
- **Consumer** — discovers suitable providers, fetches content, and verifies chunks using Merkle proofs and the publisher signature.

Content authentication proves that a chunk belongs to publisher-authenticated content. It does not, by itself, prove delivery or receipt.

## Development domains

CIPHER is divided into three independently owned domains:

- [`network/`](network/) handles peer connectivity, discovery, requests, transfer, demand metadata, cache announcements, and provider selection.
- [`availability/`](availability/) handles availability agreements, epochs, challenges, proof verification, and availability outcomes.
- [`payments/`](payments/) handles accounting, authorization, escrow, collateral, settlement, refunds, withdrawals, and payment disputes.

Smart contracts stay with the domain that owns their behavior:

- Availability-specific contracts belong in [`availability/contracts/`](availability/contracts/).
- Payment, escrow, and settlement contracts belong in [`payments/contracts/`](payments/contracts/).

## Repository layout

```text
CIPHER/
├── docs/                   Protocol and architecture documentation
├── network/                Decentralized CDN networking
├── availability/           Availability mechanisms
│   └── contracts/          Availability-specific smart contracts
├── payments/               Economic settlement
│   └── contracts/          Payment and escrow smart contracts
├── shared/                 Stable shared definitions and utilities
├── nodes/                  Publisher, provider, and consumer applications
├── integration/            Cross-domain composition and adapters
├── tests/                  Cross-module and end-to-end tests
├── scripts/                Development and operational utilities
├── config/                 Shared configuration templates
└── docker/                 Optional local container environment
```

## Dependency philosophy

- Keep `network`, `availability`, and `payments` independently implementable.
- Communicate through small, stable protocol objects or interfaces.
- Put only genuinely shared definitions in `shared`; it is not a business-logic catch-all.
- Coordinate cross-domain workflows in `integration` instead of coupling domain internals.
- Compose domain functionality into runnable roles under `nodes`.
- Keep availability decisions separate from payment settlement.

## Project status

This branch establishes the repository foundation only. Domain teams should define their internal structures as implementation decisions are made; the complete protocol is intentionally not implemented here.
