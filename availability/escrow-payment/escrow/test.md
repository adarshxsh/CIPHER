### Payment State and Settlement Test

**Tested:**

* Created and funded an escrow contract.
* Deposited provider collateral and activated the contract.
* Created a signed `PaymentState` using the publisher's Ethereum key.
* Submitted the signed payment state on-chain.
* Verified payment-state fields were stored correctly.
* Settled the contract and verified:

  * Provider received `0.4 ETH`.
  * Publisher received the remaining `0.6 ETH`.
  * Escrow balance became `0`.
  * Contract state became `Settled`.

**Result:**

`forge test -vv` →

* `testCreateContract()` — **PASS**
* `testDepositCollateralAndActivate()` — **PASS**
* `testFundEscrow()` — **PASS**
* `testSubmitPaymentStateAndSettle()` — **PASS**

**4 tests passed, 0 failed, 0 skipped.**

The test confirms Solidity payment-state signature verification, voucher acceptance, payment-state storage, and the settlement happy path.

---

### Failure Lifecycle Tests

**Tested:**

* Created, funded, and activated an escrow contract.
* Marked an active contract as failed using `AvailabilityFailure`.
* Verified the contract transitioned from `Active` to `Failed`.
* Slashed `0.2 ETH` from the provider's collateral.
* Verified:

  * Provider collateral decreased by the penalty amount.
  * Publisher received the slashed `0.2 ETH`.
  * Contract remained in the `Failed` state.
* Disputed a failure using a valid publisher-signed `PaymentState`.
* Verified:

  * Contract transitioned from `Failed` back to `Active`.
  * The signed `PaymentState` was stored correctly.
  * Sequence and cumulative payment were updated.

**Result:**

`forge test -vv` -

**EscrowFailure.t.sol**

* `testMarkFailure()` - **PASS** - (gas: 887969)
* `testSlashCollateral()` - **PASS** - (gas: 548275)
* `testDisputeFailure()` - **PASS** - (gas: 887969)
* finished in 15.57ms (12.70ms CPU time)

**EscrowContract.t.sol**

* `testCreateContract()` - **PASS** -(gas: 287987)
* `testDepositCollateralAndActivate()` - **PASS** - (gas: 457830)
* `testFundEscrow()` - **PASS** - (gas: 344284)
* `testSubmitPaymentStateAndSettle()` - **PASS**-(gas: 927913)
* finished in 15.89ms (14.45ms CPU time)

**7 tests passed, 0 failed, 0 skipped.**--Ran 2 test suites in 66.04ms (31.46ms CPU time)

The tests confirm the core escrow payment, settlement, failure, collateral-slashing, and failure-dispute lifecycle.
