const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("EscrowContract", function () {
  async function deployAgreement({ collateral = 0n } = {}) {
    const [publisher, provider, other] = await ethers.getSigners();
    const Escrow = await ethers.getContractFactory("EscrowContract");
    const escrow = await Escrow.deploy();
    const reward = ethers.parseEther("1");
    const fileID = ethers.keccak256(ethers.toUtf8Bytes("file-under-test"));
    const rulesHash = ethers.keccak256(ethers.toUtf8Bytes("rules-under-test"));
    const contractID = await escrow.createContract.staticCall(provider.address, fileID, reward, 3, 3600, rulesHash, collateral);
    await escrow.createContract(provider.address, fileID, reward, 3, 3600, rulesHash, collateral);
    return { escrow, publisher, provider, other, contractID, reward, collateral };
  }

  async function activateAgreement(fixture) {
    const { escrow, publisher, provider, contractID, reward, collateral } = fixture;
    await escrow.connect(publisher).fundEscrow(contractID, { value: reward });
    if (collateral > 0n) {
      await escrow.connect(provider).depositCollateral(contractID, { value: collateral });
    }
    await escrow.connect(publisher).activateContract(contractID);
  }

  async function signedPaymentState(fixture, { sequence = 1, payment, validUntil } = {}) {
    const { escrow, publisher, provider, contractID, reward } = fixture;
    const network = await ethers.provider.getNetwork();
    const expiry = validUntil ?? BigInt((await ethers.provider.getBlock("latest")).timestamp + 600);
    const amount = payment ?? reward;
    const challengeID = ethers.keccak256(ethers.toUtf8Bytes(`challenge-${sequence}`));
    const digest = ethers.keccak256(ethers.AbiCoder.defaultAbiCoder().encode(
      ["address", "uint256", "bytes32", "address", "address", "uint64", "uint64", "uint256", "bytes32", "uint64", "uint8"],
      [await escrow.getAddress(), network.chainId, contractID, publisher.address, provider.address, sequence, sequence, amount, challengeID, expiry, 1]
    ));
    return [contractID, publisher.address, provider.address, sequence, sequence, amount, challengeID, expiry, 1, await publisher.signMessage(ethers.getBytes(digest))];
  }

  it("creates, funds, activates, accepts a signed cumulative payment state, and settles", async function () {
    const fixture = await deployAgreement();
    const { escrow, publisher, provider, contractID, reward } = fixture;
    await activateAgreement(fixture);
    expect(await escrow.getContractState(contractID)).to.equal(2n);

    const network = await ethers.provider.getNetwork();
    const validUntil = BigInt((await ethers.provider.getBlock("latest")).timestamp + 600);
    const challengeID = ethers.keccak256(ethers.toUtf8Bytes("challenge-1"));
    const coder = ethers.AbiCoder.defaultAbiCoder();
    const digest = ethers.keccak256(coder.encode(
      ["address", "uint256", "bytes32", "address", "address", "uint64", "uint64", "uint256", "bytes32", "uint64", "uint8"],
      [await escrow.getAddress(), network.chainId, contractID, publisher.address, provider.address, 1, 1, reward, challengeID, validUntil, 1]
    ));
    const signature = await publisher.signMessage(ethers.getBytes(digest));
    const paymentState = [contractID, publisher.address, provider.address, 1, 1, reward, challengeID, validUntil, 1, signature];
    await escrow.connect(provider).submitPaymentState(contractID, paymentState);
    expect((await escrow.getPaymentState(contractID)).cumulativePayment).to.equal(reward);
    await expect(escrow.connect(provider).settleContract(contractID, paymentState))
      .to.changeEtherBalances([escrow, provider], [-reward, reward]);
    expect(await escrow.getContractState(contractID)).to.equal(4n);
  });

  it("records an eligible failure, caps collateral slashing, and refunds the publisher", async function () {
    const collateral = ethers.parseEther("0.5");
    const fixture = await deployAgreement({ collateral });
    const { escrow, publisher, contractID, reward } = fixture;
    await activateAgreement(fixture);
    await escrow.connect(publisher).markFailure(contractID, 1);
    await escrow.connect(publisher).slashCollateral(contractID, ethers.parseEther("10"));
    expect(await escrow.getCollateral(contractID)).to.equal(0n);
    await expect(escrow.connect(publisher).refundPublisher(contractID))
      .to.changeEtherBalances([escrow, publisher], [-reward, reward]);
    expect(await escrow.getContractState(contractID)).to.equal(5n);
  });

  it("rejects expired, over-reward, and replayed payment states", async function () {
    const fixture = await deployAgreement();
    const { escrow, provider, contractID, reward } = fixture;
    await activateAgreement(fixture);

    const expired = await signedPaymentState(fixture, { validUntil: 1n });
    await expect(escrow.connect(provider).submitPaymentState(contractID, expired))
      .to.be.revertedWithCustomError(escrow, "InvalidPaymentState");

    const overReward = await signedPaymentState(fixture, { payment: reward + 1n });
    await expect(escrow.connect(provider).submitPaymentState(contractID, overReward))
      .to.be.revertedWithCustomError(escrow, "InvalidPaymentState");

    const valid = await signedPaymentState(fixture, { payment: reward / 2n });
    await escrow.connect(provider).submitPaymentState(contractID, valid);
    await expect(escrow.connect(provider).submitPaymentState(contractID, valid))
      .to.be.revertedWithCustomError(escrow, "InvalidPaymentState");
  });

  it("does not let a non-publisher record a failure", async function () {
    const fixture = await deployAgreement();
    const { escrow, other, contractID } = fixture;
    await activateAgreement(fixture);

    await expect(escrow.connect(other).markFailure(contractID, 1))
      .to.be.revertedWithCustomError(escrow, "InvalidState");
  });
});
