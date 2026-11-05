# Identity and Escrow Registration

This document defines the boundary between a CIPHER Availability identity and the Ethereum identities used by the escrow/payment module.

## 1. Identity Bindings

Two explicit bindings must exist before the coordinator processes an Availability result:

| CIPHER value            | Ethereum value                | Purpose                                                                                                                                       |
| ----------------------- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Provider peer ID        | Provider Ethereum address     | Ensures payment and collateral are associated with the Ethereum account registered for the provider that answered the Availability challenge. |
| Availability ContractID | Escrow `bytes32` agreement ID | Ensures an Availability result is applied to the correct on-chain escrow agreement.                                                           |

The coordinator **must not derive the escrow agreement ID from the Availability ContractID**.

The escrow agreement ID is the authoritative `bytes32` value returned by `EscrowContract.createContract`.

The two bindings are maintained by the coordinator's `bindings.Registry`.

```go
coordinator.RegisterProvider(peerID, providerEthereumAddress)
coordinator.RegisterContract(availabilityContractID, escrowAgreementID)
```

The current registry is an **in-memory coordinator registry**. It is appropriate for the local implementation and tests, but it is not durable and is not automatically shared between nodes.

---

## 2. Provider Registration

During contract setup, the provider's CIPHER and Ethereum identities are associated as follows:

1. The provider supplies its CIPHER peer ID and Ethereum address.

2. The provider proves control of the Ethereum address by signing a registration challenge containing the required identity and freshness information.

3. The publisher verifies the signature and confirms that the recovered Ethereum address matches the submitted address.

4. The publisher creates the escrow agreement using the verified provider Ethereum address.

5. `EscrowContract.createContract` returns the authoritative escrow `bytes32` agreement ID.

6. The publisher registers both bindings with the coordinator:

```go
coordinator.RegisterProvider(peerID, providerEthereumAddress)
coordinator.RegisterContract(availabilityContractID, escrowAgreementID)
```

7. The publisher funds the escrow and the provider deposits the required collateral.

8. The agreement is activated.

9. Availability challenges should begin only after the escrow agreement is ready to accept Availability/payment operations.

The binding therefore establishes:

```text
CIPHER Provider Peer ID
        │
        ▼
Provider Ethereum Address
        │
        │ registered in
        ▼
Escrow Contract Agreement
        ▲
        │
Availability ContractID
```

The coordinator uses these bindings to translate an Availability result into the identities expected by the Ethereum escrow contract.

---

## 3. Publisher Identity and Authority

The publisher Ethereum identity is the signing authority for cumulative payment states.

For the current implementation:

* the publisher creates and funds the escrow agreement;
* the publisher signs cumulative `PaymentState` vouchers after successful `PASS` results;
* the publisher may submit `markFailure` after the Availability failure has been verified;
* the publisher may submit an eligible collateral penalty through `slashCollateral`.

The coordinator's `EscrowAdapter` therefore needs to be configured with the Ethereum account authorized to perform the corresponding publisher operations.

The same configured publisher key can be used for:

```text
PASS path:
Availability PASS
      ↓
Create PaymentState
      ↓
Publisher signs PaymentState
      ↓
Store latest voucher off-chain
      ↓
Submit/settle on-chain when required

FAIL path:
Availability FAIL
      ↓
Verify failure
      ↓
Record failure
      ↓
markFailure(...)
      ↓
slashCollateral(...) if a penalty is configured and eligible
```

Importantly, normal `PASS` processing does **not** require an immediate blockchain transaction. The signed `PaymentState` acts as an off-chain payment authorization until settlement is required.

---

## 4. Escrow Agreement Identity

The escrow agreement has two different identifiers with different responsibilities:

### Availability ContractID

This identifies the CIPHER Availability agreement used by the off-chain Availability system.

### Escrow Agreement ID

This identifies the corresponding on-chain escrow agreement.

The relationship is explicitly registered:

```text
Availability ContractID
        │
        │ explicit binding
        ▼
Escrow bytes32 Agreement ID
```

The coordinator must never assume that:

```text
Availability ContractID == Escrow Agreement ID
```

and must not derive one from the other.

The escrow ID returned by:

```solidity
createContract(...)
```

is the authoritative identifier used for subsequent escrow operations.

---

## 5. Required Registration Records

The production system should persist the registration information required to reconstruct and verify the bindings.

At minimum, the durable registry should contain:

* provider CIPHER peer ID;
* provider Ethereum address;
* provider binding proof;
* proof nonce;
* proof issue time;
* proof expiry;
* Availability ContractID;
* Ethereum chain ID;
* deployed `EscrowContract` address;
* escrow agreement `bytes32` ID returned by `createContract`;
* publisher Ethereum address;
* registration time;
* current registration/revocation status.

The registry should enforce uniqueness for:

```text
(chain ID, EscrowContract address, escrow agreement ID)
```

A provider peer ID or Ethereum address should not silently be rebound to another identity.

Any identity change should use an explicit provider-key or identity-rotation procedure.

---

## 6. Verification Before Processing an Availability Result

Before the coordinator creates or submits an escrow-related action, the relevant bindings must be verified.

The coordinator should establish that:

1. the Availability contract is registered and active;

2. the provider peer ID in the Availability result resolves to the registered Ethereum provider address;

3. the Availability ContractID resolves to the registered escrow agreement ID;

4. the result comes from the trusted Availability verification path;

5. the result has not already been processed;

6. the corresponding escrow agreement exists on the configured chain;

7. the escrow agreement's publisher and provider identities correspond to the registered identities;

8. the escrow agreement is in a state compatible with the requested operation.

The identity mapping therefore becomes:

```text
AvailabilityResult
      │
      ├── ProviderID ─────────────► Provider Ethereum Address
      │
      └── ContractID ────────────► Escrow Agreement ID
                                      │
                                      ▼
                              Ethereum Escrow Contract
```

This prevents an Availability result for one provider or contract from being accidentally applied to another escrow agreement.

---

## 7. PASS Processing

A verified `PASS` result may create a new cumulative `PaymentState`.

The coordinator:

1. verifies the Availability result;
2. resolves the provider Ethereum address from the provider binding;
3. resolves the escrow agreement ID from the Availability ContractID binding;
4. checks the previous stored payment state;
5. requires a strictly increasing sequence;
6. requires cumulative payment to be monotonic;
7. creates the new `PaymentState`;
8. sets the escrow agreement ID and Ethereum provider address;
9. signs the state with the publisher's Ethereum signing key;
10. stores the signed state as the latest payment state.

The payment state is **not automatically submitted to Ethereum** during normal PASS processing.

This preserves the optimistic/off-chain payment design.

The latest valid voucher can later be submitted during settlement.

---

## 8. FAIL Processing

A verified `FAIL` result does not create a payment authorization.

Instead, the failure is handled through the failure path:

```text
Availability FAIL
       ↓
Failure verification
       ↓
Failure record / failure streak
       ↓
Publisher-authorized markFailure
       ↓
Failed escrow state
       ↓
Optional collateral penalty
```

The current off-chain dispatcher is responsible for tracking consecutive failure results.

Once the configured failure condition is satisfied, the coordinator can expose the failure as eligible for the corresponding on-chain failure handling.

The Solidity contract does **not independently reconstruct the off-chain Availability failure streak**. The publisher submits the resulting failure action on-chain.

A collateral penalty is applied only when a penalty has explicitly been configured and the on-chain contract is in a state that permits the penalty.

The current contract also provides a provider dispute path through `disputeFailure`, allowing a provider to submit a valid signed payment state and return the agreement to the active state when the failure conditions are satisfied.

---

## 9. Separation of Responsibilities

The identity layer, Availability layer, coordinator, and escrow contract have separate responsibilities.

```text
┌─────────────────────────────┐
│ CIPHER Availability System   │
│                             │
│ Peer ID                     │
│ Availability ContractID     │
│ PASS / FAIL verification    │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Publisher Coordinator       │
│                             │
│ Provider binding            │
│ Contract binding            │
│ PaymentState creation       │
│ Failure coordination        │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Escrow Adapter              │
│                             │
│ Go ↔ Ethereum conversion    │
│ Transaction submission      │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ EscrowContract              │
│                             │
│ Escrow funds                │
│ Provider collateral         │
│ Payment settlement          │
│ Failure state               │
│ Dispute                     │
│ Slashing                    │
└─────────────────────────────┘
```

The coordinator therefore does not replace the escrow contract's state validation. It prepares and translates the off-chain Availability decision into the appropriate Ethereum operation.

Likewise, the escrow contract does not know the CIPHER peer ID. It operates on the registered Ethereum identities and escrow agreement ID.

---

## 10. Current Trust Boundary

The current implementation treats the publisher-run coordinator as the authority that connects:

```text
Availability identity
        ↕
Coordinator binding
        ↕
Ethereum identity
        ↕
Escrow agreement
```

This is suitable for the current prototype and local implementation.

However, the coordinator's in-memory `bindings.Registry` is not itself a durable or decentralized source of truth.

For production deployment, the system should define how bindings survive:

* coordinator restart;
* publisher replacement;
* provider identity rotation;
* database/node failure;
* conflicting registration attempts;
* dispute or recovery.

---

## 11. Production Evolution

The current publisher-run coordinator can later be replaced or backed by a stronger registration authority without changing the fundamental identity model.

Possible future authorities include:

* a smart-contract registry;
* a multisignature/governance registry;
* an attested validator set;
* another authenticated registration service.

The important invariant remains:

```text
Provider Peer ID
      ↕
Provider Ethereum Address

Availability ContractID
      ↕
Escrow Agreement ID
```

Before production deployment, the failure path should also have an explicitly defined dispute/attestation policy.

In the current implementation, the publisher submits `markFailure` after off-chain Availability verification. The Solidity contract therefore trusts the publisher's authorized failure submission rather than independently proving the complete Availability failure history.

A production design should define what evidence allows a failure to be accepted, challenged, and ultimately resolved.
