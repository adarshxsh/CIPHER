# Payments

## Owns

- Cumulative payment state, accounting, and authorization.
- Escrow, payment collateral, settlement, withdrawals, and refunds.
- Replay and stale-state protection, payment disputes, and payment smart contracts.

## Does not own

- Peer discovery, request transport, or caching behavior.
- Availability epochs, challenge selection, or proof verification.
- The decision that an availability obligation passed or failed.

The payments team owns this domain's internal structure, including [`contracts/`](contracts/).
