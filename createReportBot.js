// createReportBot.js
// Updated for openOracle v0.1.6 which includes an extra "settlerReward" parameter

require('dotenv').config();
const { Web3 } = require('web3');

/*
  createReportBot.js - Updated for new smart contract v0.1.6

  Instructions:
   1. Make sure you have PRIVATE_KEY set in .env or environment: 
      PRIVATE_KEY=0x123abc...
   2. Replace your contract address, RPC, etc. in the USER CONFIG AREA below.
   3. Adjust the `immediateSend` flag to:
      - true  => Will send one createReportInstance immediately and exit.
      - false => Will loop indefinitely with random intervals (on average ~1 tx/hour).
   4. Run: node createReportBot.js
*/

// ========== USER CONFIG AREA ==========

// The address of your deployed openOracle contract (v0.1.6).
const CONTRACT_ADDRESS = "0x0cd32fA8CB9F38aa7332F7A8dBeB258FB91226AB"; // <--- Update as needed

// Your Arbitrum RPC endpoint:
const RPC_ENDPOINT = "https://arb1.arbitrum.io/rpc";

// If true, sends one createReportInstance immediately, then exits.
// If false, loops with random intervals (on average ~1 tx/hour).
const immediateSend = true;

// Oracle parameters (same as before)
const token1Address = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1";
const token2Address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
const exactToken1Report = "3630000000000000";  // e.g. 0.00363 ETH in wei
const feePercentage = "2222";                  // 4 bps in thousandths of a basis point (4000 = 0.04%)
const multiplier = "110";                   // 1.1x (110%)
const settlementTime = "8";                   // 18 seconds
const escalationHalt = "1000000000000000000000"; // 1000 ETH in wei
const disputeDelay = "0";                      // 1 second
const protocolFee = "1000";                    // 1 bp (0.01%) in thousandths of a basis point

// The new parameter for v0.1.6 - settlerReward (in wei).
// Must be strictly less than the total msg.value you send.
const SETTLER_REWARD_WEI = Web3.utils.toWei("0.0001", "ether"); 

// Payable value in Wei. Must be > settlerReward or transaction will revert.
const ETH_VALUE_WEI = Web3.utils.toWei("0.0003", "ether");

// ========== END USER CONFIG AREA ==========

// Function to create random delays (Poisson-like distribution, ~1 tx/hour)
function getRandomDelaySeconds() {
  const U = Math.random();
  const randomExp = -Math.log(1 - U);
  const averageIntervalSec = 3600;
  return Math.floor(randomExp * averageIntervalSec);
}

async function main() {
  // Setup web3
  const web3 = new Web3(RPC_ENDPOINT);

  // Load private key from environment
  const PRIVATE_KEY = process.env.PRIVATE_KEY;
  if (!PRIVATE_KEY || !PRIVATE_KEY.startsWith("0x")) {
    console.error("Error: PRIVATE_KEY not set or missing 0x prefix in .env or environment!");
    process.exit(1);
  }

  // Create account from private key
  const account = web3.eth.accounts.privateKeyToAccount(PRIVATE_KEY);
  web3.eth.accounts.wallet.add(account);
  web3.eth.defaultAccount = account.address;

  // ABI with updated createReportInstance method (10 parameters now)
  const abi = [
    {
      "inputs": [
        { "internalType": "address", "name": "token1Address", "type": "address" },
        { "internalType": "address", "name": "token2Address", "type": "address" },
        { "internalType": "uint256", "name": "exactToken1Report", "type": "uint256" },
        { "internalType": "uint256", "name": "feePercentage", "type": "uint256" },
        { "internalType": "uint256", "name": "multiplier", "type": "uint256" },
        { "internalType": "uint256", "name": "settlementTime", "type": "uint256" },
        { "internalType": "uint256", "name": "escalationHalt", "type": "uint256" },
        { "internalType": "uint256", "name": "disputeDelay", "type": "uint256" },
        { "internalType": "uint256", "name": "protocolFee", "type": "uint256" },
        { "internalType": "uint256", "name": "settlerReward", "type": "uint256" }
      ],
      "name": "createReportInstance",
      "outputs": [{ "internalType": "uint256", "name": "reportId", "type": "uint256" }],
      "stateMutability": "payable",
      "type": "function"
    }
  ];

  const contract = new web3.eth.Contract(abi, CONTRACT_ADDRESS);

  // Function to send a single createReportInstance transaction
  async function sendCreateReportTx() {
    console.log("Sending createReportInstance transaction with v0.1.6 parameters...");

    const txData = contract.methods
      .createReportInstance(
        token1Address,
        token2Address,
        exactToken1Report,
        feePercentage,
        multiplier,
        settlementTime,
        escalationHalt,
        disputeDelay,
        protocolFee,
        SETTLER_REWARD_WEI
      )
      .encodeABI();

    // Build transaction
    const gasPrice = await web3.eth.getGasPrice();
    const tx = {
      from: account.address,
      to: CONTRACT_ADDRESS,
      value: ETH_VALUE_WEI,
      data: txData,
      // Slight gasPrice buffer
      gasPrice: Math.floor(Number(gasPrice) * 1.05),
      // Hard-coded gas limit (adjust as needed after testing)
      gas: 1000000,
    };

    console.log("Estimated gas price (wei):", tx.gasPrice);
    console.log("Sending tx with value (wei):", tx.value, "and settlerReward (wei):", SETTLER_REWARD_WEI);

    // Sign and send
    const signedTx = await web3.eth.accounts.signTransaction(tx, account.privateKey);
    const receipt = await web3.eth.sendSignedTransaction(signedTx.rawTransaction);

    console.log("Transaction hash:", receipt.transactionHash);
    console.log("Receipt status:", receipt.status);
    return receipt;
  }

  if (immediateSend) {
    console.log("Immediate send mode enabled. Sending one createReportInstance tx now...");
    try {
      await sendCreateReportTx();
      console.log("Done. Exiting.");
      process.exit(0);
    } catch (err) {
      console.error("Error sending tx:", err);
      process.exit(1);
    }
  } else {
    // Loop mode: random intervals
    while (true) {
      try {
        const delaySec = getRandomDelaySeconds();
        console.log(`Next createReportInstance in ~${delaySec} seconds...`);
        await new Promise((r) => setTimeout(r, delaySec * 1000));
        await sendCreateReportTx();
      } catch (err) {
        console.error("Error in loop:", err);
      }
    }
  }
}

main().catch((err) => {
  console.error("Fatal error in main():", err);
  process.exit(1);
});