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
Ran 4 tests for test/EscrowContract.t.sol:EscrowContractTest
[PASS] testCreateContract() (gas: 287987)
[PASS] testDepositCollateralAndActivate() (gas: 457830)
[PASS] testFundEscrow() (gas: 344284)
[PASS] testSubmitPaymentStateAndSettle() (gas: 927913)
Suite result: ok. 4 passed; 0 failed; 0 skipped; finished in 15.45ms (13.26ms CPU time)

Ran 1 test suite in 62.39ms (15.45ms CPU time): 4 tests passed, 0 failed, 0 skipped (4 total tests)

The test confirms the Solidity payment-state signature verification, voucher acceptance, storage, and settlement happy path.
