const { ethers } = require("hardhat");

async function main() {
  const [deployer] = await ethers.getSigners();
  const EscrowContract = await ethers.getContractFactory("EscrowContract");
  const escrow = await EscrowContract.deploy();
  await escrow.waitForDeployment();

  console.log("EscrowContract deployed");
  console.log("network:", (await ethers.provider.getNetwork()).name);
  console.log("chain ID:", (await ethers.provider.getNetwork()).chainId.toString());
  console.log("deployer:", deployer.address);
  console.log("address:", await escrow.getAddress());
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
