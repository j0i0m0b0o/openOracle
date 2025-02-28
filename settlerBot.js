require('dotenv').config();
const { Web3 } = require('web3');

/************************************************************
 *                    USER CONFIG
 ************************************************************/
const PRIVATE_KEY = process.env.PRIVATE_KEY;
if (!PRIVATE_KEY || !PRIVATE_KEY.startsWith('0x')) {
  console.error('Error: PRIVATE_KEY not set or missing "0x" prefix in environment!');
  process.exit(1);
}

// Arbitrum RPC
const ARBITRUM_RPC = 'https://arb1.arbitrum.io/rpc';
const web3 = new Web3(ARBITRUM_RPC);

// Contract address
const CONTRACT_ADDRESS = '0x0cd32fA8CB9F38aa7332F7A8dBeB258FB91226AB';

// Poll intervals
let lastBlockChecked = 0;
const EVENT_POLL_INTERVAL_MS = 5000; // 5 seconds between event checks
const MIN_POLL_INTERVAL_MS = 5000; // Minimum interval
const MAX_POLL_INTERVAL_MS = 120000; // Maximum backoff (2 minutes)
const BACKOFF_MULTIPLIER = 2; // Multiply interval by this factor on errors

// Current backoff state
let currentPollInterval = MIN_POLL_INTERVAL_MS;
let consecutiveRateErrors = 0;

// In-memory tracking of reports
const settledReports = new Set(); // Reports we've already settled
const pendingSettlements = new Map(); // reportId -> timestamp
const SETTLEMENT_LOCK_EXPIRY_MS = 60000; // 1 minute lock expiry

// Rate-limited reports queue
const rateLimitedReports = new Map(); // reportId -> {retryCount, nextRetryTime, settleAfter}
const MAX_RETRIES = 5;
const RETRY_BACKOFF_BASE_MS = 3000; // Start with 3 seconds

// Track the timestamp of when reports were processed
const reportTimestamps = new Map();

// Memory management constants
const MEMORY_CLEANUP_INTERVAL_MS = 1800000; // 30 minutes
const MAX_SETTLED_REPORTS = 5000; // Max number of reports to keep in memory
const REPORT_AGE_THRESHOLD_MS = 7 * 86400000; // 7 days - clear older reports

// Set a minimum priority fee to ensure transactions don't get stuck
const MIN_PRIORITY_FEE_GWEI = 0.1; // 0.1 gwei minimum
const MIN_PRIORITY_FEE_WEI = BigInt(Math.floor(MIN_PRIORITY_FEE_GWEI * 1e9));

// Settlement scheduling
const scheduledSettles = new Map(); // reportId -> {timestamp, timeoutId}
const SCHEDULER_CHECK_INTERVAL_MS = 30000; // Check scheduler every 30 seconds

/************************************************************
 *           CREATE LOCAL ACCOUNT
 ************************************************************/
const account = web3.eth.accounts.privateKeyToAccount(PRIVATE_KEY);
web3.eth.accounts.wallet.add(account);
web3.eth.defaultAccount = account.address;

/************************************************************
 *         openOracle v0.1.6 ABI (settle-related)
 ************************************************************/
const openOracleAbi = [
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "uint256", "name": "reportId",       "type": "uint256" },
      { "indexed": true,  "internalType": "address", "name": "token1Address",  "type": "address" },
      { "indexed": true,  "internalType": "address", "name": "token2Address",  "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "feePercentage",  "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "multiplier",     "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "exactToken1Report","type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "ethFee",         "type": "uint256" },
      { "indexed": false, "internalType": "address", "name": "creator",        "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "settlementTime", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "escalationHalt", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "disputeDelay",   "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "protocolFee",    "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "settlerReward",  "type": "uint256" }
    ],
    "name": "ReportInstanceCreated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "internalType": "uint256", "name": "reportId", "type": "uint256" },
      { "indexed": false, "internalType": "address", "name": "reporter", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "amount1", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "amount2", "type": "uint256" },
      { "indexed": true, "internalType": "address", "name": "token1Address", "type": "address" },
      { "indexed": true, "internalType": "address", "name": "token2Address", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "swapFee", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "protocolFee", "type": "uint256" }
    ],
    "name": "InitialReportSubmitted",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "internalType": "uint256", "name": "reportId", "type": "uint256" },
      { "indexed": false, "internalType": "address", "name": "disputer", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "newAmount1", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "newAmount2", "type": "uint256" },
      { "indexed": true, "internalType": "address", "name": "token1Address", "type": "address" },
      { "indexed": true, "internalType": "address", "name": "token2Address", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "swapFee", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "protocolFee", "type": "uint256" }
    ],
    "name": "ReportDisputed",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "internalType": "uint256", "name": "reportId", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "price", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "settlementTimestamp", "type": "uint256" }
    ],
    "name": "ReportSettled",
    "type": "event"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "reportId", "type": "uint256" }
    ],
    "name": "settle",
    "outputs": [
      { "internalType": "uint256", "name": "price", "type": "uint256" },
      { "internalType": "uint256", "name": "settlementTimestamp", "type": "uint256" }
    ],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs":[{"internalType":"uint256","name":"","type":"uint256"}],
    "name":"reportMeta",
    "outputs":[
      {"internalType":"address","name":"token1","type":"address"},
      {"internalType":"address","name":"token2","type":"address"},
      {"internalType":"uint256","name":"feePercentage","type":"uint256"},
      {"internalType":"uint256","name":"multiplier","type":"uint256"},
      {"internalType":"uint256","name":"settlementTime","type":"uint256"},
      {"internalType":"uint256","name":"exactToken1Report","type":"uint256"},
      {"internalType":"uint256","name":"fee","type":"uint256"},
      {"internalType":"uint256","name":"escalationHalt","type":"uint256"},
      {"internalType":"uint256","name":"disputeDelay","type":"uint256"},
      {"internalType":"uint256","name":"protocolFee","type":"uint256"},
      {"internalType":"uint256","name":"settlerReward","type":"uint256"}
    ],
    "stateMutability":"view",
    "type":"function"
  },
  {
    "inputs":[{"internalType":"uint256","name":"","type":"uint256"}],
    "name":"reportStatus",
    "outputs":[
      {"internalType":"uint256","name":"currentAmount1","type":"uint256"},
      {"internalType":"uint256","name":"currentAmount2","type":"uint256"},
      {"internalType":"address payable","name":"currentReporter","type":"address"},
      {"internalType":"address payable","name":"initialReporter","type":"address"},
      {"internalType":"uint256","name":"reportTimestamp","type":"uint256"},
      {"internalType":"uint256","name":"settlementTimestamp","type":"uint256"},
      {"internalType":"uint256","name":"price","type":"uint256"},
      {"internalType":"bool","name":"isSettled","type":"bool"},
      {"internalType":"bool","name":"disputeOccurred","type":"bool"},
      {"internalType":"bool","name":"isDistributed","type":"bool"},
      {"internalType":"uint256","name":"lastDisputeBlock","type":"uint256"}
    ],
    "stateMutability":"view",
    "type":"function"
  },
  {
    "inputs":[],
    "name":"nextReportId",
    "outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
    "stateMutability":"view",
    "type":"function"
  }
];

const openOracleContract = new web3.eth.Contract(openOracleAbi, CONTRACT_ADDRESS);

// Helper to format Wei and ensure safe JSON serialization
function formatWei(wei) {
  return {
    wei: wei.toString(),
    gwei: Number(wei) / 1e9,
    eth: Number(wei) / 1e18
  };
}

// Helper function to stringify objects with BigInt values
function safeStringify(obj) {
  return JSON.stringify(obj, (key, value) => 
    typeof value === 'bigint' ? value.toString() : value
  , 2);
}

/************************************************************
 *   Rate Limiting & Backoff Management
 ************************************************************/
function increaseBackoff() {
  consecutiveRateErrors++;
  // Apply exponential backoff (multiply by 2 each time)
  currentPollInterval = Math.min(currentPollInterval * BACKOFF_MULTIPLIER, MAX_POLL_INTERVAL_MS);
  console.log(`RATE LIMIT BACKOFF: Increasing poll interval to ${currentPollInterval}ms (${consecutiveRateErrors} consecutive errors)`);
}

function decreaseBackoff() {
  if (consecutiveRateErrors > 0 || currentPollInterval > MIN_POLL_INTERVAL_MS) {
    // Reset error count
    consecutiveRateErrors = 0;
    // Gradually step down the interval (more gentle than immediate reset)
    currentPollInterval = Math.max(MIN_POLL_INTERVAL_MS, Math.floor(currentPollInterval / BACKOFF_MULTIPLIER));
    console.log(`RATE LIMIT RECOVERY: Decreasing poll interval to ${currentPollInterval}ms`);
  }
}

// Function to determine if error is a rate limit related
function isRateLimitError(error) {
  if (!error) return false;
  
  const errorMessage = error.message || '';
  const errorCode = error.code;
  
  return (
    errorMessage.includes("Too Many Requests") || 
    errorMessage.includes("429") || 
    errorMessage.includes("rate limit") || 
    errorMessage.includes("rate exceeded") || 
    errorMessage.includes("throttled") || 
    errorCode === 429
  );
}

// Add a report to retry queue
function queueReportForRetry(reportId, settleAfter) {
  const retryInfo = rateLimitedReports.get(reportId) || { 
    retryCount: 0, 
    nextRetryTime: 0, 
    settleAfter: settleAfter || 0 
  };
  
  retryInfo.retryCount++;
  
  // Calculate next retry time with exponential backoff
  const backoffMs = RETRY_BACKOFF_BASE_MS * Math.pow(2, retryInfo.retryCount - 1);
  retryInfo.nextRetryTime = Date.now() + backoffMs;
  
  rateLimitedReports.set(reportId, retryInfo);
  
  console.log(`Queued reportId=${reportId} for retry #${retryInfo.retryCount} in ${backoffMs}ms (at ${new Date(retryInfo.nextRetryTime).toISOString()})`);
  
  // Remove from pending to allow retry
  pendingSettlements.delete(reportId);
}

// Process rate-limited reports that are ready for retry
async function processRateLimitedReports() {
  if (rateLimitedReports.size === 0) return;
  
  const now = Date.now();
  const reportsToRetry = [];
  
  // Collect reports that are ready for retry
  for (const [reportId, retryInfo] of rateLimitedReports.entries()) {
    if (now >= retryInfo.nextRetryTime) {
      // Only retry if it's time to settle or the settleAfter time is 0 (immediate)
      if (retryInfo.settleAfter === 0 || now >= retryInfo.settleAfter) {
        reportsToRetry.push({ reportId, retryInfo });
      }
    }
  }
  
  // Process each ready report
  for (const { reportId, retryInfo } of reportsToRetry) {
    console.log(`Retrying reportId=${reportId} (attempt #${retryInfo.retryCount} of ${MAX_RETRIES})`);
    
    // Remove from retry queue - will be added back if it fails again
    rateLimitedReports.delete(reportId);
    
    if (retryInfo.retryCount > MAX_RETRIES) {
      console.log(`Reached maximum retry attempts (${MAX_RETRIES}) for reportId=${reportId}, abandoning`);
      continue;
    }
    
    // Process the report with retry data
    await trySettleReport(reportId);
  }
}

/************************************************************
 *   EIP-1559 TX BUILDER (with proportional priority fee)
 ************************************************************/
async function buildEip1559Tx(txData, gasLimit) {
  try {
    const block = await web3.eth.getBlock("latest");
    if (!block.baseFeePerGas) {
      // fallback for non-EIP1559 chains
      const gasPriceStr = await web3.eth.getGasPrice();
      const gasPrice = BigInt(gasPriceStr);
      
      // Ensure minimum priority fee
      const priorityFee = gasPrice / 10n; // 10% of gas price
      const finalPriorityFee = priorityFee > MIN_PRIORITY_FEE_WEI ? priorityFee : MIN_PRIORITY_FEE_WEI;
      
      console.log(`Using legacy gas pricing, gas price: ${formatWei(gasPrice).gwei} gwei, priority: ${formatWei(finalPriorityFee).gwei} gwei`);
      
      const txParams = {
        gas: gasLimit,
        gasPrice: gasPriceStr,
        ...txData
      };
      console.log(`Legacy transaction parameters:`, safeStringify(txParams));
      return txParams;
    }
    
    const baseFeeBn = BigInt(block.baseFeePerGas);
    console.log(`Current base fee: ${formatWei(baseFeeBn).gwei} gwei`);
    
    // Calculate priority fee as 10% of base fee, with a minimum
    const calculatedPriorityBn = baseFeeBn / 10n;
    const maxPriorityBn = calculatedPriorityBn > MIN_PRIORITY_FEE_WEI ? calculatedPriorityBn : MIN_PRIORITY_FEE_WEI;
    console.log(`Max priority fee: ${formatWei(maxPriorityBn).gwei} gwei (10% of base fee or minimum ${MIN_PRIORITY_FEE_GWEI} gwei)`);
    
    // +5% buffer on base fee
    const maxFeePerGasBn = baseFeeBn + (baseFeeBn / 20n) + maxPriorityBn;
    console.log(`Max fee per gas: ${formatWei(maxFeePerGasBn).gwei} gwei (base + 5% + priority)`);

    const maxFeePerGasStr = maxFeePerGasBn.toString();
    const maxPriorityStr = maxPriorityBn.toString();

    const txParams = {
      type: '0x2',
      gas: gasLimit,
      maxFeePerGas: maxFeePerGasStr,
      maxPriorityFeePerGas: maxPriorityStr,
      ...txData
    };
    console.log(`EIP-1559 transaction parameters:`, safeStringify(txParams));
    return txParams;
  } catch (error) {
    console.error(`Error building EIP-1559 transaction:`, error);
    
    // Check for rate limiting
    if (isRateLimitError(error)) {
      increaseBackoff();
      console.warn(`Rate limit encountered while building transaction: ${error.message}`);
    }
    throw error;
  }
}

/************************************************************
 *   CLEAN UP PENDING TRANSACTIONS & SCHEDULED SETTLES
 ************************************************************/
function cleanupExpiredEntries() {
  const now = Date.now();
  
  // Clean up expired pending settlements
  for (const [reportId, timestamp] of pendingSettlements.entries()) {
    if (now - timestamp > SETTLEMENT_LOCK_EXPIRY_MS) {
      pendingSettlements.delete(reportId);
      console.log(`Removed expired settlement lock for reportId=${reportId}`);
    }
  }
  
  // Remove expired scheduled settles (those more than 1 hour late)
  const SCHEDULE_GRACE_PERIOD_MS = 60 * 60 * 1000; // 1 hour
  for (const [reportId, scheduleInfo] of scheduledSettles.entries()) {
    if (now > scheduleInfo.timestamp + SCHEDULE_GRACE_PERIOD_MS) {
      // Clear the timeout if it exists
      if (scheduleInfo.timeoutId) {
        clearTimeout(scheduleInfo.timeoutId);
      }
      
      scheduledSettles.delete(reportId);
      console.log(`Removed expired scheduled settlement for reportId=${reportId}`);
    }
  }
}

/************************************************************
 *   MEMORY MANAGEMENT: Purge old data to prevent leaks
 ************************************************************/
function purgeOldData() {
  const now = Date.now();
  console.log(`Running memory cleanup...`);
  
  // 1. Clean up expired entries
  cleanupExpiredEntries();
  
  // 2. Clean up oldest settled reports if we have too many
  if (settledReports.size > MAX_SETTLED_REPORTS) {
    // Sort by timestamp (oldest first)
    const sortedReportIds = [...reportTimestamps.entries()]
      .sort((a, b) => a[1] - b[1])
      .map(entry => entry[0]);
    
    // Calculate how many to remove
    const reportsToRemove = settledReports.size - MAX_SETTLED_REPORTS;
    
    // Remove oldest reports
    for (let i = 0; i < reportsToRemove && i < sortedReportIds.length; i++) {
      const oldReportId = sortedReportIds[i];
      settledReports.delete(oldReportId);
      reportTimestamps.delete(oldReportId);
    }
    
    console.log(`Removed ${reportsToRemove} oldest settled reports from memory`);
  }
  
  // 3. Clean up old reports by age
  let agePurgeCount = 0;
  for (const [reportId, timestamp] of reportTimestamps.entries()) {
    if (now - timestamp > REPORT_AGE_THRESHOLD_MS) {
      settledReports.delete(reportId);
      reportTimestamps.delete(reportId);
      agePurgeCount++;
    }
  }
  
  // 4. Clean up stale retry reports that exceeded max retries
  let retryPurgeCount = 0;
  for (const [reportId, retryInfo] of rateLimitedReports.entries()) {
    if (retryInfo.retryCount > MAX_RETRIES || now - retryInfo.nextRetryTime > REPORT_AGE_THRESHOLD_MS) {
      rateLimitedReports.delete(reportId);
      retryPurgeCount++;
    }
  }
  
  // Log cleanup results
  console.log(`Memory cleanup complete: Removed ${agePurgeCount} old reports and ${retryPurgeCount} stale retry reports`);
  console.log(`Current memory usage: ${settledReports.size} settled reports, ${pendingSettlements.size} pending settlements, ${rateLimitedReports.size} queued retries, ${scheduledSettles.size} scheduled settles`);
  
  // Force garbage collection if available (Node.js with --expose-gc flag)
  if (global.gc) {
    console.log(`Triggering garbage collection...`);
    global.gc();
  }
}

/************************************************************
 *   Check if a report is settleable
 ************************************************************/
async function isReportSettleable(reportId) {
  try {
    const status = await openOracleContract.methods.reportStatus(reportId).call();
    
    // Skip if already settled or distributed
    if (status.isSettled || status.isDistributed) {
      return { settleable: false, reason: 'already settled' };
    }
    
    // Must have a report timestamp and a current reporter
    if (status.reportTimestamp === '0' || status.currentReporter === '0x0000000000000000000000000000000000000000') {
      return { settleable: false, reason: 'no report submitted' };
    }
    
    const meta = await openOracleContract.methods.reportMeta(reportId).call();
    const settleTimeSeconds = Number(meta.settlementTime);
    const reportTimestamp = Number(status.reportTimestamp);
    const now = Math.floor(Date.now() / 1000);
    
    // Important: If this report has been disputed, the reportTimestamp will be updated to the 
    // time of the last dispute, which effectively resets the settlement clock
    const wasDisputed = status.disputeOccurred;
    
    // Check if settlement time has passed
    if (now < reportTimestamp + settleTimeSeconds) {
      return { 
        settleable: false, 
        reason: 'not yet settleable', 
        settleTime: reportTimestamp + settleTimeSeconds,
        settleAfter: (reportTimestamp + settleTimeSeconds) * 1000, // in milliseconds
        wasDisputed: wasDisputed
      };
    }
    
    // Settlement is possible, but we need to be aware of the extra window
    // The contract has a 60-second window where settlement behavior might be different
    const withinPostSettlementWindow = now <= reportTimestamp + settleTimeSeconds + 60;
    
    return { 
      settleable: true,
      settlerReward: meta.settlerReward,
      withinPostSettlementWindow: withinPostSettlementWindow,
      wasDisputed: wasDisputed
    };
  } catch (error) {
    console.error(`Error checking if report ${reportId} is settleable:`, error);
    
    // Check if this is a rate limit error
    if (isRateLimitError(error)) {
      throw new Error(`Rate limited while checking if report ${reportId} is settleable`);
    }
    
    return { settleable: false, reason: 'error checking status' };
  }
}

/************************************************************
 *   Try to settle a report
 ************************************************************/
async function trySettleReport(reportId) {
  const idNum = Number(reportId);
  
  try {
    // Skip if we've already settled or are in the process of settling this report
    if (settledReports.has(idNum)) {
      console.log(`Skipping already settled reportId=${idNum}`);
      return;
    }
    
    if (pendingSettlements.has(idNum)) {
      console.log(`Skipping reportId=${idNum}, settlement is pending`);
      return;
    }
    
    // Mark as pending to prevent concurrent settlement attempts
    pendingSettlements.set(idNum, Date.now());
    console.log(`Attempting to settle reportId=${idNum}`);
    
    // Check if the report is settleable
    let settleableInfo;
    try {
      settleableInfo = await isReportSettleable(idNum);
    } catch (error) {
      if (error.message && error.message.includes('Rate limited')) {
        queueReportForRetry(idNum, 0); // Retry immediately when rate limit resolves
        return;
      }
      pendingSettlements.delete(idNum);
      return;
    }
    
    if (!settleableInfo.settleable) {
      console.log(`Report ${idNum} is not settleable: ${settleableInfo.reason}`);
      
      // If not yet settleable but we have a future settle time, schedule it
      if (settleableInfo.reason === 'not yet settleable' && settleableInfo.settleAfter) {
        // If this report was disputed, make sure to log that information
        if (settleableInfo.wasDisputed) {
          console.log(`Report ${idNum} was disputed, which reset the settlement clock. New settlement time: ${new Date(settleableInfo.settleAfter).toISOString()}`);
        }
        
        scheduleSettlement(idNum, settleableInfo.settleAfter);
      }
      
      pendingSettlements.delete(idNum);
      return;
    }
    
    console.log(`Report ${idNum} is settleable! Settler reward: ${web3.utils.fromWei(settleableInfo.settlerReward, 'ether')} ETH`);
    
    // Log whether we're settling within the post-settlement window
    if (settleableInfo.withinPostSettlementWindow) {
      console.log(`Settling within the post-settlement window (first 60 seconds after settlement time)`);
    } else {
      console.log(`Settling after the post-settlement window (more than 60 seconds after settlement time)`);
    }
    
    // If the report was disputed, log that information
    if (settleableInfo.wasDisputed) {
      console.log(`Note: This report was previously disputed, which reset the settlement clock`);
    }
    
    // Build the transaction data
    const settleData = openOracleContract.methods.settle(idNum).encodeABI();
    
    // Estimate gas for the settlement transaction
    let gasEstimate;
    try {
      console.log(`Estimating gas for settlement...`);
      gasEstimate = await web3.eth.estimateGas({
        from: account.address,
        to: CONTRACT_ADDRESS,
        data: settleData
      });
      console.log(`Gas estimate: ${gasEstimate} gas units`);
    } catch (gasErr) {
      console.error(`Error estimating gas for reportId=${idNum}:`, gasErr.message);
      
      if (isRateLimitError(gasErr)) {
        // Queue for retry if rate limited
        queueReportForRetry(idNum, 0);
        return;
      }
      
      pendingSettlements.delete(idNum);
      return;
    }
    
    // Build EIP-1559 transaction
    let finalTx;
    try {
      const eip1559TxData = {
        from: account.address,
        to: CONTRACT_ADDRESS,
        data: settleData
      };
      
      finalTx = await buildEip1559Tx(eip1559TxData, gasEstimate);
    } catch (txBuildErr) {
      console.error(`Error building transaction:`, txBuildErr.message);
      
      if (isRateLimitError(txBuildErr)) {
        // Queue for retry if rate limited
        queueReportForRetry(idNum, 0);
        return;
      }
      
      pendingSettlements.delete(idNum);
      return;
    }
    
    // Calculate gas cost and check if profitable
    let gasCostWei;
    if (finalTx.maxFeePerGas) {
      gasCostWei = BigInt(finalTx.maxFeePerGas) * BigInt(gasEstimate);
    } else {
      gasCostWei = BigInt(finalTx.gasPrice) * BigInt(gasEstimate);
    }
    
    const settlerRewardWei = BigInt(settleableInfo.settlerReward);
    
    console.log(`Settlement economics for reportId=${idNum}:`);
    console.log(`  Gas cost: ${formatWei(gasCostWei).eth} ETH`);
    console.log(`  Settler reward: ${formatWei(settlerRewardWei).eth} ETH`);
    console.log(`  Profit: ${formatWei(settlerRewardWei - gasCostWei).eth} ETH`);
    
    // Only proceed if profitable
    if (gasCostWei >= settlerRewardWei) {
      console.log(`UNPROFITABLE: Skipping reportId=${idNum}, gas cost (${formatWei(gasCostWei).eth} ETH) >= reward (${formatWei(settlerRewardWei).eth} ETH)`);
      
      // Calculate a better retry delay based on whether we're settling within the window
      let retryDelay;
      if (settleableInfo.withinPostSettlementWindow) {
        // If we're in the 60-second window, retry more frequently
        const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;
        retryDelay = Date.now() + FIFTEEN_MINUTES_MS;
      } else {
        // After the window, less urgent
        const THREE_HOURS_MS = 3 * 60 * 60 * 1000;
        retryDelay = Date.now() + THREE_HOURS_MS;
      }
      
      queueReportForRetry(idNum, retryDelay);
      return;
    }
    
    // Sign and send the transaction
    console.log(`Settling reportId=${idNum}...`);
    try {
      const signedTx = await web3.eth.accounts.signTransaction(finalTx, account.privateKey);
      console.log(`Transaction signed, sending to network...`);
      
      const receipt = await web3.eth.sendSignedTransaction(signedTx.rawTransaction);
      console.log(`Successfully settled reportId=${idNum}:`);
      console.log(`  Transaction hash: ${receipt.transactionHash}`);
      console.log(`  Block number: ${receipt.blockNumber}`);
      console.log(`  Gas used: ${receipt.gasUsed}`);
      
      // Mark as settled
      settledReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      
      // Remove any scheduled settlement for this report
      if (scheduledSettles.has(idNum)) {
        const scheduleInfo = scheduledSettles.get(idNum);
        if (scheduleInfo.timeoutId) {
          clearTimeout(scheduleInfo.timeoutId);
        }
        scheduledSettles.delete(idNum);
      }
    } catch (txErr) {
      console.error(`Error sending settlement transaction for reportId=${idNum}:`, txErr.message);
      
      if (isRateLimitError(txErr)) {
        // Queue for retry if rate limited
        queueReportForRetry(idNum, 0);
        return;
      }
      
      if (txErr.message && txErr.message.includes('insufficient funds')) {
        console.error(`ERROR: Insufficient funds in wallet. Check your ETH balance.`);
      }
    }
  } catch (error) {
    console.error(`Unexpected error settling reportId=${idNum}:`, error);
    
    if (isRateLimitError(error)) {
      queueReportForRetry(idNum, 0);
      return;
    }
  } finally {
    if (pendingSettlements.has(idNum) && !rateLimitedReports.has(idNum)) {
      pendingSettlements.delete(idNum);
    }
  }
}

/************************************************************
 *   Schedule a report to be settled at a specific time
 ************************************************************/
function scheduleSettlement(reportId, settleAfterMs) {
  const idNum = Number(reportId);
  
  // Skip if already settled
  if (settledReports.has(idNum)) {
    return;
  }
  
  // Skip if already scheduled with a later timestamp
  if (scheduledSettles.has(idNum)) {
    const existing = scheduledSettles.get(idNum);
    if (existing.timestamp >= settleAfterMs) {
      return;
    }
    
    // Clear existing timeout if we're rescheduling
    if (existing.timeoutId) {
      clearTimeout(existing.timeoutId);
    }
  }
  
  const now = Date.now();
  const delayMs = Math.max(0, settleAfterMs - now);
  
  // Add 1 second buffer to ensure it's really settleable
  const actualDelayMs = delayMs + 1000;
  
  console.log(`Scheduling settlement for reportId=${idNum} in ${actualDelayMs}ms (at ${new Date(now + actualDelayMs).toISOString()})`);
  
  // Create a timeout to attempt settlement
  const timeoutId = setTimeout(() => {
    console.log(`Settlement time reached for reportId=${idNum}, attempting to settle...`);
    scheduledSettles.delete(idNum);
    trySettleReport(idNum).catch(err => {
      console.error(`Error in scheduled settlement for reportId=${idNum}:`, err);
    });
  }, actualDelayMs);
  
  // Store the schedule information
  scheduledSettles.set(idNum, {
    timestamp: settleAfterMs,
    timeoutId: timeoutId
  });
}

/************************************************************
 *   Process newly reported or disputed events
 ************************************************************/
async function processReportEvent(reportId, eventData) {
  const idNum = Number(reportId);
  
  try {
    // Skip if already settled
    if (settledReports.has(idNum)) {
      return;
    }
    
    // Check if this is a dispute event - we need special handling
    const isDisputeEvent = eventData.event === 'ReportDisputed';
    if (isDisputeEvent) {
      console.log(`Processing DISPUTE event for reportId=${idNum} - this resets the settlement clock`);
      
      // For dispute events, we should cancel any existing scheduled settlement
      if (scheduledSettles.has(idNum)) {
        const scheduleInfo = scheduledSettles.get(idNum);
        if (scheduleInfo.timeoutId) {
          clearTimeout(scheduleInfo.timeoutId);
          console.log(`Cancelled previously scheduled settlement for reportId=${idNum} due to dispute`);
        }
        scheduledSettles.delete(idNum);
      }
    } else {
      console.log(`Processing ${eventData.event || 'report'} event for reportId=${idNum}`);
    }
    
    // Check if the report is settleable or when it will be
    let settleableInfo;
    try {
      settleableInfo = await isReportSettleable(idNum);
    } catch (error) {
      if (error.message && error.message.includes('Rate limited')) {
        queueReportForRetry(idNum, 0);
        return;
      }
      return;
    }
    
    if (settleableInfo.settleable) {
      // If already settleable, try to settle immediately
      trySettleReport(idNum).catch(err => {
        console.error(`Error settling reportId=${idNum}:`, err);
      });
    } else if (settleableInfo.reason === 'not yet settleable' && settleableInfo.settleAfter) {
      // If not yet settleable, schedule for future settlement
      if (isDisputeEvent) {
        console.log(`Dispute detected - rescheduling settlement for reportId=${idNum} at new time: ${new Date(settleableInfo.settleAfter).toISOString()}`);
      }
      scheduleSettlement(idNum, settleableInfo.settleAfter);
    }
  } catch (error) {
    console.error(`Error processing event for reportId=${idNum}:`, error);
    
    if (isRateLimitError(error)) {
      queueReportForRetry(idNum, 0);
    }
  }
}

/************************************************************
 *   Check and run scheduled settlements
 ************************************************************/
async function checkScheduledSettlements() {
  try {
    const now = Date.now();
    const overdue = [];
    
    // Find all overdue settlements
    for (const [reportId, scheduleInfo] of scheduledSettles.entries()) {
      if (now >= scheduleInfo.timestamp) {
        overdue.push({ reportId, scheduleInfo });
      }
    }
    
    if (overdue.length > 0) {
      console.log(`Found ${overdue.length} overdue settlements to process`);
      
      // Process each overdue settlement
      for (const { reportId, scheduleInfo } of overdue) {
        // Clear the timeout since we're handling it now
        if (scheduleInfo.timeoutId) {
          clearTimeout(scheduleInfo.timeoutId);
        }
        
        // Remove from scheduled
        scheduledSettles.delete(reportId);
        
        // Try to settle
        console.log(`Processing overdue scheduled settlement for reportId=${reportId}`);
        await trySettleReport(reportId);
      }
    }
  } catch (error) {
    console.error(`Error checking scheduled settlements:`, error);
  }
}

/************************************************************
 *   MAIN LOGIC: Watch for events & process reports
 ************************************************************/
async function watchForEvents() {
  try {
    // Process rate-limited reports first
    await processRateLimitedReports();
    
    // Check scheduled settlements
    await checkScheduledSettlements();
    
    // Get latest block
    let latestBlock;
    try {
      latestBlock = Number(await web3.eth.getBlockNumber());
    } catch (error) {
      console.error(`Error getting latest block:`, error);
      if (isRateLimitError(error)) {
        increaseBackoff();
      }
      return;
    }
    
    // Initialize lastBlockChecked on first run
    if (lastBlockChecked === 0) {
      lastBlockChecked = latestBlock - 100; // Last 100 blocks on first run
      console.log(`First run, starting from block ${lastBlockChecked}`);
      return;
    }
    
    if (latestBlock <= lastBlockChecked) {
      return;
    }
    
    // Limit the block range to avoid processing too many blocks at once
    const MAX_BLOCKS_PER_QUERY = 100;
    const fromBlock = lastBlockChecked + 1;
    const toBlock = Math.min(latestBlock, fromBlock + MAX_BLOCKS_PER_QUERY - 1);
    
    console.log(`Checking for new events from block ${fromBlock} to ${toBlock}`);
    
    try {
      // Get InitialReportSubmitted events
      const initialReportEvents = await openOracleContract.getPastEvents('InitialReportSubmitted', {
        fromBlock,
        toBlock
      });
      
      // Get ReportDisputed events - IMPORTANT! These reset the settlement clock
      const disputeEvents = await openOracleContract.getPastEvents('ReportDisputed', {
        fromBlock,
        toBlock
      });
      
      // Get ReportSettled events
      const settledEvents = await openOracleContract.getPastEvents('ReportSettled', {
        fromBlock,
        toBlock
      });
      
      // Update lastBlockChecked if successfully got events
      lastBlockChecked = toBlock;
      
      // Decrease backoff on success
      decreaseBackoff();
      
      console.log(`Found ${initialReportEvents.length} new reports, ${disputeEvents.length} disputes, and ${settledEvents.length} settlements`);
      
      // Mark settled reports first (to avoid processing events for already settled reports)
      for (const evt of settledEvents) {
        const reportId = Number(evt.returnValues.reportId);
        console.log(`Marking reportId=${reportId} as settled (from event)`);
        settledReports.add(reportId);
        reportTimestamps.set(reportId, Date.now());
        
        // Remove any scheduled settlement for this report
        if (scheduledSettles.has(reportId)) {
          const scheduleInfo = scheduledSettles.get(reportId);
          if (scheduleInfo.timeoutId) {
            clearTimeout(scheduleInfo.timeoutId);
          }
          scheduledSettles.delete(reportId);
        }
      }
      
      // Process dispute events first since they reset settlement times
      // This ensures we correctly handle the case where a report is created and then disputed in the same block range
      for (const evt of disputeEvents) {
        if (settledReports.has(Number(evt.returnValues.reportId))) continue;
        await processReportEvent(evt.returnValues.reportId, evt);
      }
      
      // Then process initial reports
      for (const evt of initialReportEvents) {
        if (settledReports.has(Number(evt.returnValues.reportId))) continue;
        await processReportEvent(evt.returnValues.reportId, evt);
      }
    } catch (error) {
      console.error(`Error getting past events: ${error.message}`);
      
      // Check if this is a rate limit error
      if (isRateLimitError(error)) {
        // Apply exponential backoff
        increaseBackoff();
      }
      
      // Don't update lastBlockChecked to ensure we retry these blocks
      return;
    }
  } catch (err) {
    console.error('Error in watchForEvents:', err);
    
    // Check if this is a rate limit error
    if (isRateLimitError(err)) {
      // Apply exponential backoff
      increaseBackoff();
    }
  }
}

/************************************************************
 *   Periodic tasks
 ************************************************************/
async function main() {
  console.log(`Starting Settler Bot: EIP-1559 with rate limit handling`);
  console.log(`Bot account: ${account.address}`);
  
  // Check account balance at startup
  try {
    const balance = await web3.eth.getBalance(account.address);
    const balanceEth = web3.utils.fromWei(balance, 'ether');
    console.log(`Wallet balance: ${balanceEth} ETH`);
    if (Number(balanceEth) < 0.01) {
      console.warn(`WARNING: Low balance (${balanceEth} ETH). Transaction might fail.`);
    }
  } catch (err) {
    console.error(`Error checking balance: ${err.message}`);
    
    // If initial connection has rate limiting, apply backoff immediately
    if (isRateLimitError(err)) {
      increaseBackoff();
    }
  }
  
  console.log(`Event poll interval: ${MIN_POLL_INTERVAL_MS}ms (min) to ${MAX_POLL_INTERVAL_MS}ms (max)`);
  console.log(`Memory cleanup interval: ${MEMORY_CLEANUP_INTERVAL_MS}ms`);
  console.log(`Scheduler check interval: ${SCHEDULER_CHECK_INTERVAL_MS}ms`);
  console.log(`Retry system: up to ${MAX_RETRIES} retries with exponential backoff (base: ${RETRY_BACKOFF_BASE_MS}ms)`);
  
  // Initial check with a small delay to allow system to initialize
  setTimeout(() => {
    // Start watching for events with dynamic interval
    const watchInterval = setInterval(() => {
      // Clear and reset interval if backoff has been applied
      if (currentPollInterval !== MIN_POLL_INTERVAL_MS) {
        clearInterval(watchInterval);
        setInterval(() => {
          watchForEvents();
        }, currentPollInterval);
      } else {
        watchForEvents();
      }
    }, MIN_POLL_INTERVAL_MS);
    
    // Set up memory cleanup interval
    setInterval(purgeOldData, MEMORY_CLEANUP_INTERVAL_MS);
  }, 2000);
}

main();