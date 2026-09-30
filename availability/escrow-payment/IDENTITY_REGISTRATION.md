# Identity and escrow registration

This document defines the boundary between a CIPHER Availability identity and
the Ethereum identities used by the escrow/payment module.

## Bindings

Two explicit bindings must exist before the coordinator processes an
Availability result:

| CIPHER value | Ethereum value | Purpose |
| --- | --- | --- |
| Provider peer ID | Provider Ethereum address | Ensures payment and collateral are assigned to the provider that answered the Availability challenge. |
| Availability ContractID | Escrow `bytes32` agreement ID | Ensures an Availability result is applied to the correct on-chain escrow agreement. |

The coordinator never derives an escrow ID from an Availability ContractID.
The escrow ID is the `bytes32` value returned by `EscrowContract.createContract`.

## Current registration procedure

The publisher/coordinator performs the following steps during contract setup.

1. The provider supplies its CIPHER peer ID and Ethereum address.
2. The provider proves control of the Ethereum address by signing a challenge
   containing the peer ID, Ethereum address, intended chain ID, an expiry, and
   a one-time nonce.
3. The publisher verifies that EIP-191 signature and checks that the recovered
   address equals the submitted Ethereum address.
4. The publisher creates the escrow agreement with that verified provider
   address. `createContract` returns the authoritative escrow `bytes32` ID.
5. The publisher records both bindings in the coordinator:

   ```go
   coordinator.RegisterProvider(peerID, providerEthereumAddress)
   coordinator.RegisterContract(availabilityContractID, escrowAgreementID)
   ```

6. The provider deposits the required collateral and either party activates the
   funded agreement. Only then should Availability challenges begin.

The current `bindings.Registry` is an in-memory coordinator registry. It is
appropriate for the local implementation and tests, but it is not durable or
shared between nodes.

## Publisher identity and authority

The publisher Ethereum key performs three actions:

- creates and funds the escrow agreement;
- signs cumulative payment states after `PASS` results;
- submits `markFailure` and any eligible `slashCollateral` transaction after a
  verified `FAIL` result.

The `EscrowAdapter` used by a coordinator therefore must be configured with
the publisher's Ethereum key. `submitPaymentState` may be sent by any account,
but configuring the same publisher key lets the coordinator perform both PASS
and FAIL paths.

## Required records

Persist the following records in a database or signed registry before moving
past the local prototype:

- provider peer ID;
- provider Ethereum address;
- provider's signed binding proof, nonce, issue time, and expiry;
- Availability ContractID;
- chain ID;
- EscrowContract deployment address;
- escrow agreement `bytes32` ID returned by `createContract`;
- publisher Ethereum address;
- registration time and revocation status.

Use a uniqueness constraint for `(chain ID, EscrowContract address, escrow
agreement ID)` and reject a peer-ID or Ethereum-address rebinding unless an
explicit rotation procedure approves it.

## Verification before each action

Before processing a result, the coordinator must verify that:

- the Availability result belongs to a registered, active Availability
  contract;
- its provider peer ID resolves to the expected Ethereum address;
- its Availability ContractID resolves to the expected escrow agreement ID;
- the escrow agreement on the configured chain has the same publisher and
  provider addresses and is active;
- the result is from the trusted Availability verifier and has not already
  been processed.

Only a `PASS` result creates a payment authorization. A `FAIL` result records
`AvailabilityFailure` on the escrow agreement. A collateral penalty is applied
only when `SetFailurePenalty` has explicitly configured one.

## Production replacement

The publisher-run coordinator is a temporary authority. It can later be
replaced by a smart-contract registry, a multisignature/governance registry,
or an attested validator set without changing the two binding concepts above.

Before that replacement, add a dispute or validator-attestation rule around
`markFailure`; the current Solidity contract permits the publisher to mark a
failure after off-chain verification. This must be backed by a clearly defined
proof, challenge, or dispute policy in production.
