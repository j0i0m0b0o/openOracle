require('dotenv').config();
const { Web3 } = require('web3');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

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
const openOracleAddress = '0x0cd32fA8CB9F38aa7332F7A8dBeB258FB91226AB'; // replace if needed

// Poll intervals
let lastBlockChecked = 0;
const EVENT_POLL_INTERVAL_MS = 1500; // 1.5 seconds for competitive reaction time
const MIN_POLL_INTERVAL_MS = 1500; // Minimum interval
const MAX_POLL_INTERVAL_MS = 120000; // Maximum backoff (2 minutes)
const BACKOFF_MULTIPLIER = 2; // Multiply interval by this factor on errors

// Current backoff state
let currentPollInterval = MIN_POLL_INTERVAL_MS;
let consecutiveRateErrors = 0;

// In-memory set to avoid re-submitting the same reportId
const alreadySubmittedReports = new Set();
const pendingTransactions = new Map(); // reportId -> timestamp
const TRANSACTION_LOCK_EXPIRY_MS = 60000; // 1 minute

// Price caching to reduce API calls
const priceCache = {
  lastUpdate: 0,
  ethMid: null,
  usdcMid: null,
  CACHE_TTL_MS: 1000 // 1 second for fresh prices
};

// Rate limiter for Kraken API
const krakenRateLimiter = {
  lastCalled: 0,
  MIN_INTERVAL_MS: 1000 // 1 second between calls
};

// Set a minimum priority fee to ensure transactions don't get stuck
const MIN_PRIORITY_FEE_GWEI = 0.1; // 0.1 gwei minimum
const MIN_PRIORITY_FEE_WEI = BigInt(Math.floor(MIN_PRIORITY_FEE_GWEI * 1e9));

// Memory management constants
const MEMORY_CLEANUP_INTERVAL_MS = 1800000; // 30 minutes
const MAX_SUBMITTED_REPORTS = 1000; // Max number of reports to keep in memory
const REPORT_AGE_THRESHOLD_MS = 86400000; // 24 hours - clear older reports

// Track the timestamp of when reports were processed
const reportTimestamps = new Map();

// NEW: Rate-limited reports queue - to retry reports that encountered rate limits
const rateLimitedReports = new Map(); // reportId -> {retryCount, nextRetryTime, eventData}
const MAX_RETRIES = 5;
const RETRY_BACKOFF_BASE_MS = 3000; // Start with 3 seconds

/************************************************************
 *           CREATE LOCAL ACCOUNT
 ************************************************************/
const account = web3.eth.accounts.privateKeyToAccount(PRIVATE_KEY);
web3.eth.accounts.wallet.add(account);
web3.eth.defaultAccount = account.address;

/************************************************************
 *         openOracle v0.1.6 ABI (ReportInstanceCreated)
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
    "inputs": [
      { "internalType": "uint256","name": "reportId","type": "uint256" },
      { "internalType": "uint256","name": "amount1","type": "uint256" },
      { "internalType": "uint256","name": "amount2","type": "uint256" }
    ],
    "name": "submitInitialReport",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  // Function to check if report already exists
  {
    "inputs": [
      { "internalType": "uint256","name": "","type": "uint256" }
    ],
    "name": "reportStatus",
    "outputs": [
      { "name": "currentAmount1", "type": "uint256" },
      { "name": "currentAmount2", "type": "uint256" },
      { "name": "currentReporter", "type": "address" },
      { "name": "initialReporter", "type": "address" },
      { "name": "reportTimestamp", "type": "uint256" },
      { "name": "settlementTimestamp", "type": "uint256" },
      { "name": "price", "type": "uint256" },
      { "name": "isSettled", "type": "bool" },
      { "name": "disputeOccurred", "type": "bool" },
      { "name": "isDistributed", "type": "bool" },
      { "name": "lastDisputeBlock", "type": "uint256" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
];

const openOracleContract = new web3.eth.Contract(openOracleAbi, openOracleAddress);

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

// NEW: Function to determine if error is a rate limit related
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

// NEW: Add a report to retry queue
function queueReportForRetry(reportId, eventData) {
  const retryInfo = rateLimitedReports.get(reportId) || { retryCount: 0, nextRetryTime: 0, eventData };
  retryInfo.retryCount++;
  
  // Calculate next retry time with exponential backoff
  const backoffMs = RETRY_BACKOFF_BASE_MS * Math.pow(2, retryInfo.retryCount - 1);
  retryInfo.nextRetryTime = Date.now() + backoffMs;
  
  rateLimitedReports.set(reportId, retryInfo);
  
  console.log(`Queued reportId=${reportId} for retry #${retryInfo.retryCount} in ${backoffMs}ms (at ${new Date(retryInfo.nextRetryTime).toISOString()})`);
  
  // Remove from pending to allow retry
  pendingTransactions.delete(reportId);
}

// NEW: Process rate-limited reports that are ready for retry
async function processRateLimitedReports() {
  if (rateLimitedReports.size === 0) return;
  
  const now = Date.now();
  const reportsToRetry = [];
  
  // Collect reports that are ready for retry
  for (const [reportId, retryInfo] of rateLimitedReports.entries()) {
    if (now >= retryInfo.nextRetryTime) {
      reportsToRetry.push({ reportId, retryInfo });
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
    await processReportEvent(reportId, retryInfo.eventData);
  }
}

/************************************************************
 *    GET ORDER BOOK FROM KRAKEN + COMPUTE MIDPOINT with rate limiting
 ************************************************************/
async function getKrakenMid(pairName) {
  // Rate limiting
  const now = Date.now();
  const timeElapsed = now - krakenRateLimiter.lastCalled;
  if (timeElapsed < krakenRateLimiter.MIN_INTERVAL_MS) {
    const waitTime = krakenRateLimiter.MIN_INTERVAL_MS - timeElapsed;
    console.log(`Rate limiting Kraken API, waiting ${waitTime}ms`);
    await new Promise(resolve => setTimeout(resolve, waitTime));
  }
  krakenRateLimiter.lastCalled = Date.now();

  const symbolMap = {
    'ETH/USD': 'XETHZUSD',
    'USDC/USD': 'USDCUSD'
  };
  const krakenPair = symbolMap[pairName];
  if (!krakenPair) {
    throw new Error(`No Kraken pair code for ${pairName}`);
  }

  const url = `https://api.kraken.com/0/public/Depth?pair=${krakenPair}&count=1`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      // If we get rate limited by Kraken specifically
      if (resp.status === 429) {
        increaseBackoff(); // Apply global backoff on Kraken rate limits
        throw new Error(`Kraken API rate limit exceeded (429): Backoff applied`);
      }
      throw new Error(`Kraken Depth API error: ${resp.status} ${resp.statusText}`);
    }
    const data = await resp.json();
    if (data.error && data.error.length > 0) {
      throw new Error(`Kraken error: ${data.error.join(', ')}`);
    }
    const resultPair = Object.keys(data.result)[0];
    const orderBook = data.result[resultPair];
    if (!orderBook.asks || !orderBook.bids || orderBook.asks.length<1 || orderBook.bids.length<1) {
      throw new Error(`Kraken Depth: no data for ${pairName}`);
    }

    const bestAsk = parseFloat(orderBook.asks[0][0]);
    const bestBid = parseFloat(orderBook.bids[0][0]);
    return (bestAsk + bestBid)/2;
  } catch (error) {
    console.error(`Error fetching Kraken mid for ${pairName}:`, error);
    throw error;
  }
}

async function getBothMid() {
  // Check cache first
  const now = Date.now();
  if (priceCache.lastUpdate > 0 && 
      now - priceCache.lastUpdate < priceCache.CACHE_TTL_MS && 
      priceCache.ethMid !== null && 
      priceCache.usdcMid !== null) {
    console.log(`Using cached price data: ETH/USD=${priceCache.ethMid}, USDC/USD=${priceCache.usdcMid}`);
    return { ethMid: priceCache.ethMid, usdcMid: priceCache.usdcMid };
  }

  // Fetch new prices
  try {
    const [ethMid, usdcMid] = await Promise.all([
      getKrakenMid('ETH/USD'),
      getKrakenMid('USDC/USD')
    ]);
    
    // Update cache
    priceCache.lastUpdate = now;
    priceCache.ethMid = ethMid;
    priceCache.usdcMid = usdcMid;
    
    return { ethMid, usdcMid };
  } catch (error) {
    // Check for rate limiting
    if (isRateLimitError(error)) {
      increaseBackoff();
      console.warn(`Rate limit encountered while fetching prices: ${error.message}`);
    }
    
    // If fetching fails but we have cached data, use that instead with a warning
    if (priceCache.lastUpdate > 0 && priceCache.ethMid !== null && priceCache.usdcMid !== null) {
      console.warn(`Failed to fetch new prices, using stale cached data from ${new Date(priceCache.lastUpdate).toISOString()}`);
      return { ethMid: priceCache.ethMid, usdcMid: priceCache.usdcMid };
    }
    throw error;
  }
}

/************************************************************
 *   EIP-1559 TX BUILDER (with proportional priority fee)
 ************************************************************/
async function buildEip1559Tx(txData, gasLimit) {
  try {
    const block = await web3.eth.getBlock("latest");
    if (!block.baseFeePerGas) {
      // fallback
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
 *  CHECK IF REPORT ALREADY HAS INITIAL SUBMISSION
 ************************************************************/
async function checkReportHasSubmission(reportId) {
  try {
    const status = await openOracleContract.methods.reportStatus(reportId).call();
    const hasSubmission = status.currentReporter !== '0x0000000000000000000000000000000000000000';
    console.log(`Report ${reportId} status check - Has submission: ${hasSubmission}, Current reporter: ${status.currentReporter}`);
    return hasSubmission;
  } catch (error) {
    console.error(`Error checking report status for reportId=${reportId}:`, error);
    
    // Check for rate limiting
    if (isRateLimitError(error)) {
      increaseBackoff();
      console.warn(`Rate limit encountered while checking report status: ${error.message}`);
      throw error; // Rethrow so caller can handle retry logic
    }
    
    // In case of non-rate-limit error, assume report doesn't exist (safer to skip)
    return true;
  }
}

/************************************************************
 *   CLEAN UP PENDING TRANSACTIONS
 ************************************************************/
function cleanupExpiredTransactions() {
  const now = Date.now();
  for (const [reportId, timestamp] of pendingTransactions.entries()) {
    if (now - timestamp > TRANSACTION_LOCK_EXPIRY_MS) {
      pendingTransactions.delete(reportId);
      console.log(`Removed expired transaction lock for reportId=${reportId}`);
    }
  }
}

/************************************************************
 *   MEMORY MANAGEMENT: Purge old data to prevent leaks
 ************************************************************/
function purgeOldData() {
  const now = Date.now();
  console.log(`Running memory cleanup...`);
  
  // 1. Clean up old pending transactions
  let pendingPurgeCount = 0;
  for (const [reportId, timestamp] of pendingTransactions.entries()) {
    if (now - timestamp > TRANSACTION_LOCK_EXPIRY_MS) {
      pendingTransactions.delete(reportId);
      pendingPurgeCount++;
    }
  }
  
  // 2. Clean up oldest submitted reports if we have too many
  if (alreadySubmittedReports.size > MAX_SUBMITTED_REPORTS) {
    // Sort by timestamp (oldest first)
    const sortedReportIds = [...reportTimestamps.entries()]
      .sort((a, b) => a[1] - b[1])
      .map(entry => entry[0]);
    
    // Calculate how many to remove
    const reportsToRemove = alreadySubmittedReports.size - MAX_SUBMITTED_REPORTS;
    
    // Remove oldest reports
    for (let i = 0; i < reportsToRemove && i < sortedReportIds.length; i++) {
      const oldReportId = sortedReportIds[i];
      alreadySubmittedReports.delete(oldReportId);
      reportTimestamps.delete(oldReportId);
    }
    
    console.log(`Removed ${reportsToRemove} oldest submitted reports from memory`);
  }
  
  // 3. Clean up old reports by age
  let agePurgeCount = 0;
  for (const [reportId, timestamp] of reportTimestamps.entries()) {
    if (now - timestamp > REPORT_AGE_THRESHOLD_MS) {
      alreadySubmittedReports.delete(reportId);
      reportTimestamps.delete(reportId);
      agePurgeCount++;
    }
  }
  
  // 4. NEW: Clean up stale retry reports that exceeded max retries
  let retryPurgeCount = 0;
  for (const [reportId, retryInfo] of rateLimitedReports.entries()) {
    if (retryInfo.retryCount > MAX_RETRIES || now - retryInfo.nextRetryTime > REPORT_AGE_THRESHOLD_MS) {
      rateLimitedReports.delete(reportId);
      retryPurgeCount++;
    }
  }
  
  // Log cleanup results
  console.log(`Memory cleanup complete: Removed ${pendingPurgeCount} stale pending transactions, ${agePurgeCount} old reports, and ${retryPurgeCount} stale retry reports`);
  console.log(`Current memory usage: ${alreadySubmittedReports.size} submitted reports, ${pendingTransactions.size} pending transactions, ${rateLimitedReports.size} queued retries`);
  
  // Force garbage collection if available (Node.js with --expose-gc flag)
  if (global.gc) {
    console.log(`Triggering garbage collection...`);
    global.gc();
  }
}

/************************************************************
 *   NEW: Process a single report event (with retry support)
 ************************************************************/
async function processReportEvent(reportId, eventData) {
  if (!eventData) {
    console.error(`Missing event data for reportId=${reportId}`);
    return;
  }
  
  const idNum = Number(reportId);
  
  // Skip if we've already submitted or are in the process of submitting this report
  if (alreadySubmittedReports.has(idNum)) {
    console.log(`Skipping, already processed reportId=${idNum}`);
    return;
  }
  
  if (pendingTransactions.has(idNum)) {
    console.log(`Skipping, transaction is pending for reportId=${idNum}`);
    return;
  }
  
  // Track this report as being processed
  pendingTransactions.set(idNum, Date.now());
  console.log(`Processing reportId=${idNum}, locked for transaction processing`);
  
  try {
    // Check if report already has an initial submission
    const hasSubmission = await checkReportHasSubmission(idNum);
    
    if (hasSubmission) {
      console.log(`Skipping, report already has an initial submission for reportId=${idNum}`);
      alreadySubmittedReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      pendingTransactions.delete(idNum);
      return;
    }
    
    // Extract event data
    const {
      token1Address,
      token2Address,
      feePercentage,
      multiplier,
      exactToken1Report,
      ethFee,
      creator,
      settlementTime,
      escalationHalt,
      disputeDelay,
      protocolFee,
      settlerReward
    } = eventData.returnValues;
    
    // Basic checks
    const feePctNum = Number(feePercentage);
    const settTimeNum = Number(settlementTime);
    const t1 = token1Address.toLowerCase();
    const t2 = token2Address.toLowerCase();

    if (t1==='0x82af49447d8a07e3bd95bd0d56f35241523fbab1' && 
        t2==='0xaf88d065e77c8cc2239327c5edb3a432268e5831')
    {
      if (feePctNum > 2000) {
        if (settTimeNum >= 5 && settTimeNum <= 25) {
          let ethUsdMid, usdcUsdMid;
          try {
            const mids = await getBothMid();
            ethUsdMid = mids.ethMid;
            usdcUsdMid = mids.usdcMid;
            console.log(`Order book mid: ETH/USD=${ethUsdMid}, USDC/USD=${usdcUsdMid}`);
          } catch(err) {
            console.error('Error fetching price mids:', err);
            if (isRateLimitError(err)) {
              // Queue for retry if rate limited
              queueReportForRetry(idNum, eventData);
              return;
            }
            pendingTransactions.delete(idNum);
            return;
          }

          // Convert amounts
          const amount1Big = BigInt(exactToken1Report.toString());
          const wethFloat = Number(amount1Big)/1e18;
          const wethUsdValue= wethFloat * ethUsdMid;
          const usdcNeededFloat= wethUsdValue/ usdcUsdMid;
          const usdcAmount= Math.floor(usdcNeededFloat*1e6);
          
          console.log(`=== Price Calculation for reportId=${idNum} ===`);
          console.log(`WETH amount: ${wethFloat} ETH`);
          console.log(`ETH/USD price: ${ethUsdMid}`);
          console.log(`WETH USD value: $${wethUsdValue.toFixed(2)}`);
          console.log(`USDC/USD price: ${usdcUsdMid}`);
          console.log(`USDC needed: ${usdcNeededFloat.toFixed(6)} (${usdcAmount} USDC in wei)`);

          const totalEthFeeWei= BigInt(ethFee.toString());
          const settlerRewardWei= BigInt(settlerReward || '0');
          const reporterPotWei= totalEthFeeWei - settlerRewardWei;
          
          console.log(`=== Fee Calculation ===`);
          console.log(`Total ETH fee: ${formatWei(totalEthFeeWei).eth} ETH`);
          console.log(`Settler reward: ${formatWei(settlerRewardWei).eth} ETH`);
          console.log(`Reporter potential reward: ${formatWei(reporterPotWei).eth} ETH`);

          // build tx data
          const submitData= openOracleContract.methods
            .submitInitialReport(idNum, amount1Big.toString(), usdcAmount.toString())
            .encodeABI();

          // estimate gas
          let gasEstimate;
          try {
            console.log(`Estimating gas for transaction...`);
            gasEstimate = await web3.eth.estimateGas({
              from: account.address,
              to: openOracleAddress,
              data: submitData
            });
            console.log(`Gas estimate: ${gasEstimate} gas units`);
          } catch(gasErr) {
            console.error(`Error estimateGas for reportId=${idNum}:`, gasErr.message);
            if (isRateLimitError(gasErr)) {
              // Queue for retry if rate limited
              queueReportForRetry(idNum, eventData);
              return;
            }
            pendingTransactions.delete(idNum);
            return;
          }

          // Get latest block for gas calculations
          let block;
          try {
            block = await web3.eth.getBlock("latest");
          } catch (blockErr) {
            console.error(`Error getting latest block:`, blockErr.message);
            if (isRateLimitError(blockErr)) {
              // Queue for retry if rate limited
              queueReportForRetry(idNum, eventData);
              return;
            }
            pendingTransactions.delete(idNum);
            return;
          }

          // approximate cost with smaller buffer
          let baseFeeBn = BigInt(0);
          if (block.baseFeePerGas) {
            baseFeeBn = BigInt(block.baseFeePerGas);
            console.log(`Current base fee: ${formatWei(baseFeeBn).gwei} gwei`);
          } else {
            try {
              baseFeeBn = BigInt(await web3.eth.getGasPrice());
              console.log(`No base fee in block, using gas price: ${formatWei(baseFeeBn).gwei} gwei`);
            } catch (gasPriceErr) {
              console.error(`Error getting gas price:`, gasPriceErr.message);
              if (isRateLimitError(gasPriceErr)) {
                // Queue for retry if rate limited
                queueReportForRetry(idNum, eventData);
                return;
              }
              pendingTransactions.delete(idNum);
              return;
            }
          }
          
          // Priority fee is 10% of base fee, with a minimum
          const calculatedPriorityBn = baseFeeBn / 10n;
          const maxPriorityBn = calculatedPriorityBn > MIN_PRIORITY_FEE_WEI ? calculatedPriorityBn : MIN_PRIORITY_FEE_WEI;
          console.log(`Priority fee: ${formatWei(maxPriorityBn).gwei} gwei (10% of base fee or minimum ${MIN_PRIORITY_FEE_GWEI} gwei)`);
          
          // +5% buffer on base fee
          const maxFeeBn = baseFeeBn + (baseFeeBn / 20n) + maxPriorityBn;
          console.log(`Max fee: ${formatWei(maxFeeBn).gwei} gwei (base + 5% + priority)`);
          
          const gasCostWei = maxFeeBn * BigInt(gasEstimate);
          console.log(`Total gas cost: ${formatWei(gasCostWei).eth} ETH`);

          const profitWei = reporterPotWei - gasCostWei;
          console.log(`=== Profit Calculation ===`);
          console.log(`Reporter reward: ${formatWei(reporterPotWei).eth} ETH`);
          console.log(`Gas cost: ${formatWei(gasCostWei).eth} ETH`);
          console.log(`Profit: ${formatWei(profitWei).eth} ETH (${profitWei} wei)`);
          
          if (profitWei <= 0n) {
            console.log(`UNPROFITABLE: Skipping reportId=${idNum}, no profit after gas`);
            pendingTransactions.delete(idNum);
            return;
          }

          // minimal profit
          const minProfitUsd = 0.0001 * 2 * wethUsdValue;
          const minProfitEth = minProfitUsd / ethUsdMid;
          const minProfitWei = BigInt(Math.floor(minProfitEth * 1e18));
          
          console.log(`=== Minimum Profit Threshold ===`);
          console.log(`Minimum profit required: ${formatWei(minProfitWei).eth} ETH`);
          console.log(`Actual profit: ${formatWei(profitWei).eth} ETH`);
          console.log(`Profit > minimum? ${profitWei > minProfitWei ? 'YES' : 'NO'}`);
          
          if (profitWei > minProfitWei) {
            console.log(`submitInitialReport for reportId=${idNum} is profitable, sending EIP-1559 tx...`);
            try {
              const eip1559TxData = {
                from: account.address,
                to: openOracleAddress,
                data: submitData,
                gas: gasEstimate
              };
              console.log(`Building transaction with data:`, safeStringify(eip1559TxData));
              
              // Build the transaction with potential rate limit handling
              let finalTx;
              try {
                finalTx = await buildEip1559Tx(eip1559TxData, gasEstimate);
                console.log(`Final transaction parameters:`, safeStringify(finalTx));
              } catch (txBuildErr) {
                console.error(`Error building transaction:`, txBuildErr.message);
                if (isRateLimitError(txBuildErr)) {
                  // Queue for retry if rate limited
                  queueReportForRetry(idNum, eventData);
                  return;
                }
                pendingTransactions.delete(idNum);
                return;
              }

              console.log(`Signing transaction...`);
              let signedTx;
              try {
                signedTx = await web3.eth.accounts.signTransaction(finalTx, account.privateKey);
                console.log(`Transaction signed, raw tx length: ${signedTx.rawTransaction.length}`);
              } catch (signErr) {
                console.error(`Error signing transaction:`, signErr.message);
                if (isRateLimitError(signErr)) {
                  // Queue for retry if rate limited
                  queueReportForRetry(idNum, eventData);
                  return;
                }
                pendingTransactions.delete(idNum);
                return;
              }
              
              console.log(`Sending transaction to network...`);
              try {
                const receipt = await web3.eth.sendSignedTransaction(signedTx.rawTransaction);
                console.log(`Transaction success details:`, safeStringify({
                  txHash: receipt.transactionHash,
                  blockNumber: receipt.blockNumber,
                  gasUsed: receipt.gasUsed,
                  status: receipt.status
                }));
                console.log(`submitInitialReport success! id=${idNum}, txHash=${receipt.transactionHash}`);
                
                // Mark as submitted after successful transaction
                alreadySubmittedReports.add(idNum);
                reportTimestamps.set(idNum, Date.now());
              } catch (txError) {
                console.error(`Transaction error details:`, safeStringify({
                  message: txError.message,
                  code: txError.code,
                  receipt: txError.receipt || 'No receipt',
                  reason: txError.reason || 'Unknown reason'
                }));
                
                if (isRateLimitError(txError)) {
                  // Queue for retry if rate limited
                  queueReportForRetry(idNum, eventData);
                  return;
                }
                
                if (txError.message && txError.message.includes('insufficient funds')) {
                  console.error(`ERROR: Insufficient funds in wallet. Check your ETH balance.`);
                }
                
                console.error(`Error sending TX for reportId=${idNum}:`, txError.message);
              }
            } catch(sendErr) {
              console.error(`Error sending TX for reportId=${idNum}:`, sendErr.message);
              
              if (isRateLimitError(sendErr)) {
                // Queue for retry if rate limited
                queueReportForRetry(idNum, eventData);
                return;
              }
            }
          } else {
            console.log(`PROFIT THRESHOLD FAIL: reportId=${idNum}, profit ${formatWei(profitWei).eth} ETH < minimum ${formatWei(minProfitWei).eth} ETH`);
          }
        } else {
          console.log(`reportId=${idNum}, settlementTime not in [5..25], skip.`);
        }
      } else {
        console.log(`reportId=${idNum}, feePct <=2000, skip.`);
      }
    } else {
      console.log(`reportId=${idNum}, token addresses != WETH/USDC, skip.`);
    }
  } catch (error) {
    console.error(`Error processing reportId=${idNum}:`, error);
    
    // Check if this was a rate limit error
    if (isRateLimitError(error)) {
      // Queue for retry
      queueReportForRetry(idNum, eventData);
      return;
    }
  } finally {
    // Remove from pending if not queued for retry
    if (pendingTransactions.has(idNum) && !rateLimitedReports.has(idNum)) {
      pendingTransactions.delete(idNum);
    }
  }
}

/************************************************************
 *   MAIN LOGIC: checkForNewReportInstances
 ************************************************************/
async function checkForNewReportInstances() {
  try {
    // Cleanup expired transaction locks
    cleanupExpiredTransactions();
    
    // Process reports that were previously rate-limited and are ready for retry
    await processRateLimitedReports();
    
    const latestBlock = Number(await web3.eth.getBlockNumber());
    if (lastBlockChecked === 0) {
      lastBlockChecked = latestBlock - 10; // Last 10 blocks on first run
      console.log(`First run, starting from block ${lastBlockChecked}`);
      return;
    }
    if (latestBlock < lastBlockChecked) {
      console.log(`Latest block (${latestBlock}) < lastBlockChecked (${lastBlockChecked}), skipping...`);
      return;
    }

    // Limit the block range to avoid processing too many blocks at once
    const MAX_BLOCKS_PER_QUERY = 50;
    const fromBlock = lastBlockChecked + 1;
    const toBlock = Math.min(latestBlock, fromBlock + MAX_BLOCKS_PER_QUERY - 1);
    
    console.log(`Checking for new reports from block ${fromBlock} to ${toBlock}`);
    
    let events;
    try {
      events = await openOracleContract.getPastEvents('ReportInstanceCreated', {
        fromBlock,
        toBlock
      });
      
      // Only update lastBlockChecked if successfully got events
      lastBlockChecked = toBlock;
      
      // If we successfully got events, we can reduce backoff
      decreaseBackoff();
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
    
    console.log(`Found ${events.length} ReportInstanceCreated events`);
    
    // If we successfully got events, we can reduce backoff
    decreaseBackoff();
    
    // Process each event with the new approach
    for (const evt of events) {
      const { returnValues } = evt;
      const reportId = returnValues.reportId;
      console.log(`Processing event in block ${evt.blockNumber}, reportId=${reportId}`);
      
      // Process each report
      await processReportEvent(reportId, evt);
    }
    
    console.log(`Successfully processed blocks ${fromBlock} to ${toBlock}`);
  } catch(err) {
    console.error('Error in checkForNewReportInstances:', err);
    
    // Check if this is a rate limit error
    if (isRateLimitError(err)) {
      // Apply exponential backoff
      increaseBackoff();
    }
    
    // Don't update lastBlockChecked on error to ensure no blocks are missed
  }
}

/************************************************************
 *   Periodic tasks
 ************************************************************/
async function main() {
  console.log(`Starting initialReport bot: EIP-1559 with proportional priority fee`);
  console.log(`Bot account: ${account.address}`);
  
  // Check for rate limit errors in Kraken API
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
  
  console.log(`Kraken rate limiting: ${krakenRateLimiter.MIN_INTERVAL_MS}ms between calls`);
  console.log(`Price cache TTL: ${priceCache.CACHE_TTL_MS}ms`);
  console.log(`Event poll interval: ${MIN_POLL_INTERVAL_MS}ms (min) to ${MAX_POLL_INTERVAL_MS}ms (max)`);
  console.log(`Memory cleanup interval: ${MEMORY_CLEANUP_INTERVAL_MS}ms`);
  console.log(`Retry system: up to ${MAX_RETRIES} retries with exponential backoff (base: ${RETRY_BACKOFF_BASE_MS}ms)`);
  
  // Initial check with a small delay to allow system to initialize
  setTimeout(() => {
    checkForNewReportInstances();
    
    // Set up the regular interval after the first check with dynamic interval
    const pollIntervalId = setInterval(() => {
      // Clear and reset interval if backoff has been applied
      if (currentPollInterval !== EVENT_POLL_INTERVAL_MS) {
        clearInterval(pollIntervalId);
        setInterval(() => {
          checkForNewReportInstances();
        }, currentPollInterval);
      } else {
        checkForNewReportInstances();
      }
    }, EVENT_POLL_INTERVAL_MS);
    
    // Set up memory cleanup interval
    setInterval(purgeOldData, MEMORY_CLEANUP_INTERVAL_MS);
  }, 2000);
}

main();
