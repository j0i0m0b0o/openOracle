require('dotenv').config();
const { Web3 } = require('web3');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

/************************************************************
 *                    USER CONFIG
 ************************************************************/
const PRIVATE_KEY = process.env.PRIVATE_KEY;
if (!PRIVATE_KEY || !PRIVATE_KEY.startsWith('0x')) {
  console.error('Error: PRIVATE_KEY not set or missing "0x" prefix in environment!');
  process.exit(1);
}

// RPC Configuration - Multi-provider setup
const RPC_CONFIG = {
  providers: [
    {
      url: 'https://arb1.arbitrum.io/rpc',
      name: 'Arbitrum Public RPC',
      priority: 1,
      supportsBatching: null, // Will be auto-detected
      maxBatchSize: 10,
      rateLimit: {
        requestsPerSecond: 5,
        lastRequest: 0,
        consecutiveErrors: 0,
        inBackoff: false,
        backoffUntil: 0
      },
      nonce: null,
      pendingTxCount: 0,
      enabled: true
    },
    {
      url: 'https://arbitrum.blockpi.network/v1/rpc/public',
      name: 'BlockPI Public RPC',
      priority: 2,
      supportsBatching: null, // Will be auto-detected
      maxBatchSize: 10,
      rateLimit: {
        requestsPerSecond: 5,
        lastRequest: 0,
        consecutiveErrors: 0,
        inBackoff: false,
        backoffUntil: 0
      },
      nonce: null,
      pendingTxCount: 0,
      enabled: true
    },
    {
      url: '', // Third provider left blank for future use
      name: 'Custom RPC (Not Configured)',
      priority: 3,
      supportsBatching: null,
      maxBatchSize: 10,
      rateLimit: {
        requestsPerSecond: 5,
        lastRequest: 0,
        consecutiveErrors: 0,
        inBackoff: false,
        backoffUntil: 0
      },
      nonce: null,
      pendingTxCount: 0,
      enabled: false
    }
  ],
  // Current active provider index (rotated as needed)
  activeProviderIndex: 0,
};

// Initialize web3 instances for each provider
const web3Instances = [];

// Transaction queue for managing dispute transactions
const txQueue = {
  pending: [], // Array of {reportId, txData, timeRemaining, priority}
  processing: false,
  
  // Add a transaction to the queue with priority based on time remaining
  addTransaction(reportId, txData, timeRemaining) {
    // Calculate priority (lower time remaining = higher priority)
    let priority = 1;
    if (timeRemaining < 10) priority = 3; // High priority for < 10 seconds remaining
    if (timeRemaining < 5) priority = 5;  // Critical priority for < 5 seconds remaining
    
    this.pending.push({
      reportId,
      txData,
      timeRemaining,
      priority,
      addedAt: Date.now()
    });
    
    // Sort by priority first, then by time remaining
    this.sortByPriority();
    
    logger.info(`Added transaction for reportId=${reportId} to queue. Priority: ${priority}, Time remaining: ${timeRemaining}s`);
    return true;
  },
  
  // Sort queue by priority (higher number = higher priority)
  sortByPriority() {
    this.pending.sort((a, b) => {
      // First sort by priority (higher priority first)
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      
      // Then by time remaining (less time = higher priority)
      if (a.timeRemaining !== b.timeRemaining) {
        return a.timeRemaining - b.timeRemaining;
      }
      
      // Finally by time added (older first)
      return a.addedAt - b.addedAt;
    });
  },
  
  // Get the next batch of transactions to process
  getBatch(size, providerIndex) {
    if (this.pending.length === 0) return [];
    
    // Get up to 'size' items
    return this.pending.slice(0, size);
  },
  
  // Remove a transaction from the queue
  removeTransaction(reportId) {
    const initialLength = this.pending.length;
    this.pending = this.pending.filter(tx => tx.reportId !== reportId);
    return initialLength !== this.pending.length;
  },
  
  // Mark a batch of transactions as complete
  completeBatch(reportIds) {
    for (const reportId of reportIds) {
      this.removeTransaction(reportId);
    }
  },
  
  // Clean old transactions from the queue
  cleanQueue(maxAgeMs = 10 * 60 * 1000) { // Default 10 minutes
    const now = Date.now();
    const initialLength = this.pending.length;
    
    this.pending = this.pending.filter(tx => {
      return now - tx.addedAt < maxAgeMs;
    });
    
    const removedCount = initialLength - this.pending.length;
    if (removedCount > 0) {
      logger.info(`Cleaned ${removedCount} stale transactions from the queue`);
    }
    
    return removedCount;
  }
};

// Contract address
const openOracleAddress = '0x0cd32fA8CB9F38aa7332F7A8dBeB258FB91226AB';

// Poll intervals
let lastBlockChecked = 0;
const EVENT_POLL_INTERVAL_MS = 500; // 0.5 seconds for competitive reaction time
const MIN_POLL_INTERVAL_MS = 500; // Minimum interval
const MAX_POLL_INTERVAL_MS = 120000; // Maximum backoff (2 minutes)
const BACKOFF_MULTIPLIER = 2; // Multiply interval by this factor on errors

// Current backoff state
let currentPollInterval = MIN_POLL_INTERVAL_MS;
let consecutiveRateErrors = 0;

// In-memory set to avoid re-processing the same reportId
const processedReports = new Set();
const pendingDisputes = new Map(); // reportId -> timestamp
const DISPUTE_LOCK_EXPIRY_MS = 60000; // 1 minute

// Price caching to reduce API calls
const priceCache = {
  lastUpdate: 0,
  ethMid: null,
  usdcMid: null,
  CACHE_TTL_MS: 5000, // Fixed 5 seconds for fresh prices
  // Flag for when we're using extended cache due to rate limiting
  usingExtendedCache: false,
  // Is a price update in progress
  updateInProgress: false,
  // Is price fetching in backoff mode
  inBackoffMode: false,
  // Last error encountered during price fetch
  lastError: null
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
const MAX_PROCESSED_REPORTS = 1000; // Max number of reports to keep in memory
const REPORT_AGE_THRESHOLD_MS = 86400000; // 24 hours - clear older reports

// Track the timestamp of when reports were processed
const reportTimestamps = new Map();

// Rate-limited reports queue - to retry reports that encountered rate limits
const rateLimitedReports = new Map(); // reportId -> {retryCount, nextRetryTime, eventData}
const MAX_RETRIES = 5;
const RETRY_BACKOFF_BASE_MS = 3000; // Start with 3 seconds

// Minimum profitability threshold
const MIN_PROFITABILITY_THRESHOLD = 0.0001; // 0.01% minimum profitability

// New: TX System configuration for multi-provider system
const TX_SYSTEM = {
  // Transaction processing stats
  stats: {
    totalSubmitted: 0,
    totalSucceeded: 0,
    totalFailed: 0,
    batchRequests: 0,
    singleRequests: 0,
    providerStats: [] // Will be initialized based on providers
  },
  
  // Configuration options
  config: {
    maxQueueSize: 100,          // Maximum transactions to keep in queue
    batchSize: 10,              // Maximum transactions in a batch
    processingInterval: 1000,   // Process queued txs every 1 second
    nonceRefreshInterval: 10000, // Refresh nonce every 10 seconds
    parallelTxLimit: 5          // Max parallel transaction submissions per provider
  },
  
  // Pending nonce for each account
  pendingNonce: null,
  
  // Timer references for cleanup
  timers: {
    processingTimer: null,
    nonceRefreshTimer: null,
    statusLogTimer: null
  }
};

/************************************************************
 *           CREATE LOCAL ACCOUNT
 ************************************************************/
let account;

/************************************************************
 *         openOracle ABI (events and functions for disputes)
 ************************************************************/
const openOracleAbi = [
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "internalType": "uint256", "name": "reportId", "type": "uint256" },
      { "indexed": true, "internalType": "address", "name": "token1Address", "type": "address" },
      { "indexed": true, "internalType": "address", "name": "token2Address", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "feePercentage", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "multiplier", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "exactToken1Report", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "ethFee", "type": "uint256" },
      { "indexed": false, "internalType": "address", "name": "creator", "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "settlementTime", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "escalationHalt", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "disputeDelay", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "protocolFee", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "settlerReward", "type": "uint256" }
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
    "inputs": [
      { "internalType": "uint256", "name": "reportId", "type": "uint256" },
      { "internalType": "address", "name": "tokenToSwap", "type": "address" },
      { "internalType": "uint256", "name": "newAmount1", "type": "uint256" },
      { "internalType": "uint256", "name": "newAmount2", "type": "uint256" }
    ],
    "name": "disputeAndSwap",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "", "type": "uint256" }
    ],
    "name": "reportMeta",
    "outputs": [
      { "internalType": "address", "name": "token1", "type": "address" },
      { "internalType": "address", "name": "token2", "type": "address" },
      { "internalType": "uint256", "name": "feePercentage", "type": "uint256" },
      { "internalType": "uint256", "name": "multiplier", "type": "uint256" },
      { "internalType": "uint256", "name": "settlementTime", "type": "uint256" },
      { "internalType": "uint256", "name": "exactToken1Report", "type": "uint256" },
      { "internalType": "uint256", "name": "fee", "type": "uint256" },
      { "internalType": "uint256", "name": "escalationHalt", "type": "uint256" },
      { "internalType": "uint256", "name": "disputeDelay", "type": "uint256" },
      { "internalType": "uint256", "name": "protocolFee", "type": "uint256" },
      { "internalType": "uint256", "name": "settlerReward", "type": "uint256" }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "", "type": "uint256" }
    ],
    "name": "reportStatus",
    "outputs": [
      { "internalType": "uint256", "name": "currentAmount1", "type": "uint256" },
      { "internalType": "uint256", "name": "currentAmount2", "type": "uint256" },
      { "internalType": "address payable", "name": "currentReporter", "type": "address" },
      { "internalType": "address payable", "name": "initialReporter", "type": "address" },
      { "internalType": "uint256", "name": "reportTimestamp", "type": "uint256" },
      { "internalType": "uint256", "name": "settlementTimestamp", "type": "uint256" },
      { "internalType": "uint256", "name": "price", "type": "uint256" },
      { "internalType": "bool", "name": "isSettled", "type": "bool" },
      { "internalType": "bool", "name": "disputeOccurred", "type": "bool" },
      { "internalType": "bool", "name": "isDistributed", "type": "bool" },
      { "internalType": "uint256", "name": "lastDisputeBlock", "type": "uint256" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
];

let openOracleContract;

/************************************************************
 *    ENHANCED LOGGING SYSTEM
 ************************************************************/
// Enhanced logging system
const logger = {
  // Log levels
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
  
  // Log a message with a specific level
  log(level, message, data = null) {
    // Only log if the level is within our configured level
    if (level > CONFIG.LOG_LEVEL) return;
    
    const timestamp = new Date().toISOString();
    let prefix = '';
    
    switch(level) {
      case this.ERROR:
        prefix = '[ERROR]';
        break;
      case this.WARN:
        prefix = '[WARN] ';
        break;
      case this.INFO:
        prefix = '[INFO] ';
        break;
      case this.DEBUG:
        prefix = '[DEBUG]';
        break;
    }
    
    // Log the message with timestamp and prefix
    console.log(`${timestamp} ${prefix} ${message}`);
    
    // If we have additional data and detailed logging is enabled, log that too
    if (data !== null && CONFIG.DETAILED_LOGGING) {
      if (typeof data === 'object') {
        console.log(safeStringify(data, null, 2));
      } else {
        console.log(data);
      }
    }
  },
  
  // Helper methods for different log levels
  error(message, data = null) {
    this.log(this.ERROR, message, data);
  },
  
  warn(message, data = null) {
    this.log(this.WARN, message, data);
  },
  
  info(message, data = null) {
    this.log(this.INFO, message, data);
  },
  
  debug(message, data = null) {
    this.log(this.DEBUG, message, data);
  },
  
  // Enhanced error logging for contract interactions
  contractError(actionType, reportId, error) {
    // Try to extract a meaningful error message
    let errorMessage = error.message || 'Unknown error';
    let errorReason = null;
    let errorDetails = {};
    
    // Check if there's a transaction hash we can use
    const txHash = error.transactionHash || null;
    
    // Try to extract revert reason if present
    if (errorMessage.includes('revert') && error.data) {
      try {
        // Sometimes the revert reason is in error.reason
        if (error.reason) {
          errorReason = error.reason;
        }
        // Sometimes it's encoded in the error data
        else if (typeof error.data === 'string' && error.data.startsWith('0x08c379a0')) {
          // This is the function selector for Error(string)
          const abiCoder = new web3Instances[0].eth.abi.constructor();
          const data = error.data.slice(10); // Remove function selector
          const decodedData = abiCoder.decodeParameter('string', data);
          errorReason = decodedData;
        }
      } catch (decodeError) {
        errorReason = 'Could not decode revert reason';
      }
    }
    
    // Check if it's an out-of-gas error
    if (errorMessage.includes('gas') && errorMessage.includes('exceed')) {
      errorReason = 'Transaction likely ran out of gas';
    }
    
    // Check if it's a nonce error
    if (errorMessage.includes('nonce')) {
      errorReason = 'Nonce issue - transaction might be queued or replaced';
    }
    
    // Construct details object
    errorDetails = {
      reportId,
      action: actionType,
      errorMessage,
      reason: errorReason,
      txHash,
      code: error.code,
      stack: error.stack?.substring(0, 300) // First part of stack trace
    };
    
    // Log it with our enhanced logger
    this.error(`Failed ${actionType} for reportId=${reportId}: ${errorReason || errorMessage}`, errorDetails);
    
    return errorDetails;
  }
};

// User configuration
const CONFIG = {
  // Creator filter mode
  CREATOR_FILTER_ENABLED: false, // Set to true to enable creator filtering
  
  // Filter by creator addresses (used only when CREATOR_FILTER_ENABLED is true)
  CREATOR_FILTER: [], // e.g. ['0x123...', '0x456...']
  
  // Price update configuration
  PRICE_UPDATE_INTERVAL_MS: 1100, // 1.1 seconds between Kraken API calls
  MAX_PRICE_CACHE_AGE_MS: 30000, // Maximum 30 seconds before cache is considered too old
  
  // Enable detailed logging
  DETAILED_LOGGING: true,
  
  // Log verbosity level (1-3)
  // 1 = minimal logs, 2 = normal logs, 3 = debug logs
  LOG_LEVEL: 2
};

// Store report settlement times
const reportSettlementInfo = {
  // reportId -> {creationTime, settlementTime, escalationHalt, disputeDelay}
  reports: new Map(),
  
  // Add a new report
  addReport(reportId, data) {
    if (!this.reports.has(reportId)) {
      this.reports.set(reportId, {
        createdAt: Math.floor(Date.now() / 1000),
        ...data
      });
      return true;
    }
    return false;
  },
  
  // Get settlement info for a report
  getInfo(reportId) {
    return this.reports.get(reportId);
  },
  
  // Calculate time remaining until settlement window closes
  getRemainingTime(reportId, currentTimestamp, reportTimestamp) {
    const info = this.getInfo(reportId);
    if (!info) return null;
    
    // Calculate when settlement time will be reached
    const settlementWindow = reportTimestamp + info.settlementTime;
    return settlementWindow - currentTimestamp; // Seconds remaining
  },
  
  // Clean up old reports to prevent memory bloat
  cleanupOldReports() {
    const now = Math.floor(Date.now() / 1000);
    const MAX_AGE_SECONDS = 3 * 24 * 60 * 60; // 3 days
    let removedCount = 0;
    
    for (const [reportId, info] of this.reports.entries()) {
      const age = now - info.createdAt;
      if (age > MAX_AGE_SECONDS) {
        this.reports.delete(reportId);
        removedCount++;
      }
    }
    
    if (removedCount > 0) {
      logger.info(`Cleaned up ${removedCount} old report settlement info entries`);
    }
    
    return removedCount;
  }
};

// Event queue system for processing reports in batches
const eventQueue = {
  pendingReports: [], // Array of {reportId, eventData, timeRemaining}
  processing: false,  // Flag to prevent concurrent processing
  
  // Add an event to the queue with calculated time remaining
  addEvent(reportId, eventData, reportTimestamp) {
    // Don't add duplicate reports
    if (this.pendingReports.some(item => item.reportId === reportId)) {
      return false;
    }
    
    // Calculate time remaining to settlement (if we have the info)
    const now = Math.floor(Date.now() / 1000);
    let timeRemaining = Infinity;
    
    const settlementInfo = reportSettlementInfo.getInfo(reportId);
    if (settlementInfo && reportTimestamp) {
      timeRemaining = reportSettlementInfo.getRemainingTime(reportId, now, reportTimestamp);
      
      // If no time remaining or already past settlement, don't queue
      if (timeRemaining !== null && timeRemaining <= 0) {
        logger.info(`Not queuing reportId=${reportId}, settlement time has passed`);
        return false;
      }
    }
    
    this.pendingReports.push({
      reportId,
      eventData,
      timeRemaining: timeRemaining || Infinity,
      addedAt: now
    });
    
    return true;
  },
  
  // Sort by time remaining (less time = higher priority)
  sortByTimeRemaining() {
    this.pendingReports.sort((a, b) => {
      // If time remaining is the same or unknown, preserve order
      if (a.timeRemaining === b.timeRemaining) return 0;
      
      // Put finite times (known settlement windows) before infinite (unknown)
      if (a.timeRemaining === Infinity) return 1;
      if (b.timeRemaining === Infinity) return -1;
      
      // Sort by time remaining (ascending = less time first)
      return a.timeRemaining - b.timeRemaining;
    });
  },
  
  // Process a batch of events
  async processBatch(batchSize) {
    if (this.processing || this.pendingReports.length === 0) {
      return 0;
    }
    
    this.processing = true;
    let processedCount = 0;
    
    try {
      // Sort by time remaining before processing
      this.sortByTimeRemaining();
      
      // Log the queue state with time remaining info
      logger.info(`Dispute queue status: ${this.pendingReports.length} reports pending`);
      this.pendingReports.slice(0, 5).forEach((item, idx) => {
        const timeLabel = item.timeRemaining === Infinity ? 
          "unknown time remaining" : 
          `${item.timeRemaining}s remaining`;
        logger.info(`  ${idx+1}. Report #${item.reportId}: ${timeLabel}`);
      });
      
      // Process up to batchSize events
      const batch = this.pendingReports.slice(0, batchSize);
      
      // Get price data once for the entire batch
      let priceData;
      try {
        priceData = await getBothMid();
        logger.info(`Using price data for batch: ETH/USD=${priceData.ethMid}, USDC/USD=${priceData.usdcMid}`);
      } catch (error) {
        logger.error('Failed to get price data for batch:', error);
        
        if (isRateLimitError(error)) {
          increaseBackoff();
          
          // If we have cached data, use it despite being stale
          if (priceCache.ethMid !== null && priceCache.usdcMid !== null) {
            logger.info(`EXTENDED CACHE: Using stale price data due to rate limits`);
            priceCache.usingExtendedCache = true;
            priceData = { 
              ethMid: priceCache.ethMid, 
              usdcMid: priceCache.usdcMid 
            };
          } else {
            // We can't process without price data
            return 0;
          }
        } else {
          // For non-rate limit errors, just return
          return 0;
        }
      }
      
      // Process each report in the batch with the same price data
      for (const item of batch) {
        try {
          await processReportWithPrices(item.reportId, item.eventData, priceData);
          processedCount++;
          
          // Remove from pending queue
          const index = this.pendingReports.indexOf(item);
          if (index > -1) {
            this.pendingReports.splice(index, 1);
          }
        } catch (error) {
          logger.error(`Error processing report ${item.reportId} in batch:`, error);
          // Leave in queue to retry later if it wasn't a rate limit error
          if (!isRateLimitError(error)) {
            const index = this.pendingReports.indexOf(item);
            if (index > -1) {
              this.pendingReports.splice(index, 1);
            }
          }
        }
      }
      
      return processedCount;
    } finally {
      this.processing = false;
      
      // Log queue status
      logger.info(`Batch processed ${processedCount} reports. ${this.pendingReports.length} reports remain in queue.`);
    }
  },
  
  // Clear old events from the queue
  cleanQueue(maxAgeMs = 10 * 60 * 1000) { // Default 10 minutes
    const now = Math.floor(Date.now() / 1000);
    const initialLength = this.pendingReports.length;
    
    this.pendingReports = this.pendingReports.filter(item => {
      return now - item.addedAt < maxAgeMs;
    });
    
    const removedCount = initialLength - this.pendingReports.length;
    if (removedCount > 0) {
      logger.info(`Cleaned ${removedCount} stale reports from the queue`);
    }
    
    return removedCount;
  }
};

/************************************************************
 *   HELPER FUNCTIONS
 ************************************************************/
// Helper function to stringify objects with BigInt values
function safeStringify(obj) {
  return JSON.stringify(obj, (key, value) => 
    typeof value === 'bigint' ? value.toString() : value
  , 2);
}

// Helper to format Wei and ensure safe JSON serialization
function formatWei(wei) {
  return {
    wei: wei.toString(),
    gwei: Number(wei) / 1e9,
    eth: Number(wei) / 1e18
  };
}

/************************************************************
 *   Rate Limiting & Backoff Management
 ************************************************************/
function increaseBackoff() {
  consecutiveRateErrors++;
  // Apply exponential backoff (multiply by 2 each time)
  currentPollInterval = Math.min(currentPollInterval * BACKOFF_MULTIPLIER, MAX_POLL_INTERVAL_MS);
  logger.warn(`RATE LIMIT BACKOFF: Increasing poll interval to ${currentPollInterval}ms (${consecutiveRateErrors} consecutive errors)`);
}

function decreaseBackoff() {
  if (consecutiveRateErrors > 0 || currentPollInterval > MIN_POLL_INTERVAL_MS) {
    // Reset error count
    consecutiveRateErrors = 0;
    // Gradually step down the interval (more gentle than immediate reset)
    currentPollInterval = Math.max(MIN_POLL_INTERVAL_MS, Math.floor(currentPollInterval / BACKOFF_MULTIPLIER));
    logger.info(`RATE LIMIT RECOVERY: Decreasing poll interval to ${currentPollInterval}ms`);
  }
}

// Function to determine if error is a rate limit related
function isRateLimitError(error) {
  if (!error) return false;
  
  const errorMessage = (error.message || '').toLowerCase();
  const errorCode = error.code;
  
  return (
    errorMessage.includes("too many requests") || 
    errorMessage.includes("429") || 
    errorMessage.includes("rate limit") || 
    errorMessage.includes("rate exceeded") || 
    errorMessage.includes("throttled") || 
    errorCode === 429
  );
}

// Add a report to retry queue
function queueReportForRetry(reportId, eventData, retryDelay) {
  const retryInfo = rateLimitedReports.get(reportId) || { retryCount: 0, nextRetryTime: 0, eventData };
  retryInfo.retryCount++;
  
  // Calculate next retry time
  let delayMs;
  
  if (retryDelay !== undefined) {
    // If retryDelay is provided, use it directly as milliseconds to wait
    delayMs = retryDelay;
  } else {
    // Otherwise use exponential backoff
    delayMs = RETRY_BACKOFF_BASE_MS * Math.pow(2, retryInfo.retryCount - 1);
  }
  
  retryInfo.nextRetryTime = Date.now() + delayMs;
  
  rateLimitedReports.set(reportId, retryInfo);
  
  logger.info(`Queued reportId=${reportId} for retry #${retryInfo.retryCount} in ${delayMs}ms (at ${new Date(retryInfo.nextRetryTime).toISOString()})`);
  
  // Remove from pending to allow retry
  pendingDisputes.delete(reportId);
}

// Process rate-limited reports that are ready for retry
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
    logger.info(`Retrying reportId=${reportId} (attempt #${retryInfo.retryCount} of ${MAX_RETRIES})`);
    
    // Remove from retry queue - will be added back if it fails again
    rateLimitedReports.delete(reportId);
    
    if (retryInfo.retryCount > MAX_RETRIES) {
      logger.info(`Reached maximum retry attempts (${MAX_RETRIES}) for reportId=${reportId}, abandoning`);
      continue;
    }
    
    // Check if this is a dispute retry with pre-calculated parameters
    if (retryInfo.eventData && retryInfo.eventData.disputeParams) {
      // This is a dispute transaction that was delayed due to dispute window not being open yet
      const { disputeParams, timeRemaining } = retryInfo.eventData;
      logger.info(`Retrying dispute transaction for reportId=${reportId} with pre-calculated parameters`);
      
      // Calculate updated timeRemaining based on original value and time passed
      const elapsedSeconds = Math.floor((now - retryInfo.nextRetryTime + retryInfo.retryCount * 1000) / 1000);
      const updatedTimeRemaining = Math.max(1, timeRemaining - elapsedSeconds);
      
      // Attempt to build and queue the transaction again
      await buildAndQueueDisputeTx(reportId, disputeParams, updatedTimeRemaining);
    } else {
      // Standard report processing retry
      await processReport(reportId, retryInfo.eventData);
    }
  }
}

/************************************************************
 *   JSON-RPC BATCHING IMPLEMENTATION
 ************************************************************/
// Initialize the multi-provider transaction system
function initializeRpcSystem() {
  // Initialize web3 instances for each provider
  for (let i = 0; i < RPC_CONFIG.providers.length; i++) {
    const provider = RPC_CONFIG.providers[i];
    
    // Skip providers that are not enabled or don't have a URL
    if (!provider.enabled || !provider.url) {
      web3Instances[i] = null;
      continue;
    }
    
    // Create web3 instance
    web3Instances[i] = new Web3(provider.url);
    
    // Initialize stats for this provider
    TX_SYSTEM.stats.providerStats[i] = {
      name: provider.name,
      totalSubmitted: 0,
      totalSucceeded: 0,
      totalFailed: 0,
      batchRequests: 0,
      singleRequests: 0,
      lastUsed: 0,
      avgResponseTime: 0,
      nonceErrors: 0
    };
  }
  
  // Explicitly ensure nonce tracking is initialized
  if (!TX_SYSTEM.nonce) {
    TX_SYSTEM.nonce = {
      value: null,
      lastRefreshed: 0,
      pendingCount: 0,
      consecutiveErrors: 0,
      locallyIncremented: 0
    };
  }
  
  // Load account for all web3 instances
  account = web3Instances[0].eth.accounts.privateKeyToAccount(PRIVATE_KEY);
  for (let i = 0; i < web3Instances.length; i++) {
    if (web3Instances[i]) {
      web3Instances[i].eth.accounts.wallet.add(account);
      web3Instances[i].eth.defaultAccount = account.address;
    }
  }
  
  // Start with an immediate nonce refresh
  refreshNonce(true);
  
  // Start the nonce refresh timer
  TX_SYSTEM.timers.nonceRefreshTimer = setInterval(refreshNonce, TX_SYSTEM.config.nonceRefreshInterval);
  
  // Start transaction processing timer
  TX_SYSTEM.timers.processingTimer = setInterval(processTransactionQueue, TX_SYSTEM.config.processingInterval);
  
  // Start status logging timer
  TX_SYSTEM.timers.statusLogTimer = setInterval(logTxSystemStatus, 60000); // Log status every minute
  
  // Initialize contract instances
  openOracleContract = new web3Instances[0].eth.Contract(openOracleAbi, openOracleAddress);
  
  logger.info(`RPC System initialized with ${RPC_CONFIG.providers.filter(p => p.enabled).length} providers`);
}

// Refresh nonce for the account with improved reliability
async function refreshNonce(forceRefresh = false) {
  // Make sure the nonce object is initialized
  if (!TX_SYSTEM.nonce) {
    TX_SYSTEM.nonce = {
      value: null,
      lastRefreshed: 0,
      pendingCount: 0,
      consecutiveErrors: 0,
      locallyIncremented: 0
    };
  }

  // Skip if recently refreshed and not forced
  if (!forceRefresh && 
      TX_SYSTEM.nonce.value !== null && 
      Date.now() - TX_SYSTEM.nonce.lastRefreshed < 2000 &&
      TX_SYSTEM.nonce.consecutiveErrors < TX_SYSTEM.config.nonceCheckThreshold) {
    return;
  }
  
  // Use all available providers to try to get the nonce
  let latestNonce = null;
  let successfulProvider = null;
  
  // Try each provider until we get a successful response
  for (let i = 0; i < RPC_CONFIG.providers.length; i++) {
    if (!RPC_CONFIG.providers[i].enabled) continue;
    
    try {
      const web3 = web3Instances[i];
      // Get the actual on-chain nonce
      const onChainNonce = await web3.eth.getTransactionCount(account.address, 'pending');
      
      latestNonce = BigInt(onChainNonce);
      successfulProvider = i;
      
      // Once we have a successful nonce query, break out
      break;
    } catch (error) {
      logger.warn(`Failed to get nonce from provider ${RPC_CONFIG.providers[i].name}: ${error.message}`);
      
      // Apply backoff if rate limited
      if (isRateLimitError(error)) {
        applyProviderBackoff(i);
      }
    }
  }
  
  // If we couldn't get the nonce from any provider, log and return
  if (latestNonce === null) {
    logger.error(`Failed to refresh nonce from any provider`);
    return;
  }
  
  const previousNonce = TX_SYSTEM.nonce.value;
  
  // Update the global nonce tracking
  TX_SYSTEM.nonce.value = latestNonce;
  TX_SYSTEM.nonce.lastRefreshed = Date.now();
  TX_SYSTEM.nonce.locallyIncremented = 0;
  
  // Log significant changes in nonce
  if (previousNonce !== null && latestNonce > previousNonce) {
    logger.info(`Nonce updated from ${previousNonce} to ${latestNonce} via provider ${RPC_CONFIG.providers[successfulProvider].name}`);
    
    // If the difference is large, it might indicate missed transactions
    if (latestNonce - previousNonce > 1) {
      logger.warn(`Nonce jumped by ${latestNonce - previousNonce} - external transactions may have occurred`);
    }
  } else if (previousNonce === null) {
    logger.info(`Initial nonce set to ${latestNonce} via provider ${RPC_CONFIG.providers[successfulProvider].name}`);
  }
  
  // Reset consecutive errors since we successfully refreshed
  TX_SYSTEM.nonce.consecutiveErrors = 0;
  
  // Update nonce for all providers to keep them in sync
  for (const provider of RPC_CONFIG.providers) {
    if (provider.enabled) {
      provider.nonce = latestNonce;
    }
  }
}

// Get the next available provider
function getNextAvailableProvider() {
  const now = Date.now();
  
  // Try to maintain the active provider if possible
  const currentIndex = RPC_CONFIG.activeProviderIndex;
  const currentProvider = RPC_CONFIG.providers[currentIndex];
  
  // If current provider is available, use it
  if (currentProvider.enabled && 
      !currentProvider.rateLimit.inBackoff &&
      currentProvider.pendingTxCount < TX_SYSTEM.config.parallelTxLimit) {
    return currentIndex;
  }
  
  // Find the next available provider
  for (let i = 0; i < RPC_CONFIG.providers.length; i++) {
    if (i === currentIndex) continue; // Skip current provider
    
    const provider = RPC_CONFIG.providers[i];
    if (provider.enabled && 
        !provider.rateLimit.inBackoff &&
        provider.pendingTxCount < TX_SYSTEM.config.parallelTxLimit) {
      
      // Update active provider index
      RPC_CONFIG.activeProviderIndex = i;
      return i;
    }
  }
  
  // If all providers are in backoff, find the one with earliest backoff expiry
  let earliestBackoff = Infinity;
  let earliestIndex = -1;
  
  for (let i = 0; i < RPC_CONFIG.providers.length; i++) {
    const provider = RPC_CONFIG.providers[i];
    if (provider.enabled && provider.rateLimit.backoffUntil < earliestBackoff) {
      earliestBackoff = provider.rateLimit.backoffUntil;
      earliestIndex = i;
    }
  }
  
  // If we found a provider and its backoff has expired, use it
  if (earliestIndex !== -1 && earliestBackoff <= now) {
    // Reset backoff
    RPC_CONFIG.providers[earliestIndex].rateLimit.inBackoff = false;
    RPC_CONFIG.providers[earliestIndex].rateLimit.backoffUntil = 0;
    RPC_CONFIG.activeProviderIndex = earliestIndex;
    return earliestIndex;
  }
  
  // If all providers are in active backoff, return -1
  return -1;
}

// Apply backoff to a provider that hit rate limits
function applyProviderBackoff(providerIndex) {
  const provider = RPC_CONFIG.providers[providerIndex];
  provider.rateLimit.consecutiveErrors++;
  
  // Calculate backoff time based on consecutive errors
  const backoffTimeMs = Math.min(
    1000 * Math.pow(2, provider.rateLimit.consecutiveErrors),
    60000 // Max 1 minute backoff
  );
  
  provider.rateLimit.inBackoff = true;
  provider.rateLimit.backoffUntil = Date.now() + backoffTimeMs;
  
  logger.warn(`Provider ${provider.name} rate limited. Backing off for ${backoffTimeMs}ms`);
}

// Reset backoff for a provider after successful requests
function resetProviderBackoff(providerIndex) {
  const provider = RPC_CONFIG.providers[providerIndex];
  if (provider.rateLimit.consecutiveErrors > 0) {
    provider.rateLimit.consecutiveErrors = 0;
    provider.rateLimit.inBackoff = false;
    provider.rateLimit.backoffUntil = 0;
    logger.info(`Reset backoff for provider ${provider.name}`);
  }
}

// Execute JSON-RPC batch request
async function executeBatchRequest(providerIndex, batchRequest) {
  const provider = RPC_CONFIG.providers[providerIndex];
  const startTime = Date.now();
  
  try {
    // Make fetch request with JSON-RPC batch
    const response = await fetch(provider.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(batchRequest)
    });
    
    // Update rate limit tracking
    provider.rateLimit.lastRequest = Date.now();
    
    // Handle rate limiting
    if (response.status === 429) {
      applyProviderBackoff(providerIndex);
      throw new Error(`Provider ${provider.name} returned 429 Too Many Requests`);
    }
    
    // Handle other errors
    if (!response.ok) {
      throw new Error(`Provider ${provider.name} returned ${response.status}: ${response.statusText}`);
    }
    
    // Parse response
    const batchResponse = await response.json();
    
    // Calculate response time for stats
    const responseTime = Date.now() - startTime;
    updateProviderStats(providerIndex, 'batch', true, responseTime);
    
    // Reset backoff after successful request
    resetProviderBackoff(providerIndex);
    
    // If we get here, this provider supports batching
    if (provider.supportsBatching === null) {
      provider.supportsBatching = true;
      logger.info(`Provider ${provider.name} supports JSON-RPC batching`);
    }
    
    return batchResponse;
  } catch (error) {
    // Calculate response time for stats
    const responseTime = Date.now() - startTime;
    updateProviderStats(providerIndex, 'batch', false, responseTime);
    
    // Check if this might indicate that batching is not supported
    if (provider.supportsBatching === null && 
        (error.message.includes('batch') || 
         error.message.includes('method not found') ||
         error.message.includes('invalid') ||
         error.message.includes('unprocessable'))) {
      provider.supportsBatching = false;
      logger.warn(`Provider ${provider.name} does not support JSON-RPC batching, will use individual requests`);
    }
    
    // Apply backoff if rate limited
    if (isRateLimitError(error)) {
      applyProviderBackoff(providerIndex);
    }
    
    throw error;
  }
}

// Update provider stats
function updateProviderStats(providerIndex, requestType, success, responseTime) {
  const stats = TX_SYSTEM.stats.providerStats[providerIndex];
  if (!stats) return;
  
  stats.totalSubmitted++;
  stats.lastUsed = Date.now();
  
  if (success) {
    stats.totalSucceeded++;
  } else {
    stats.totalFailed++;
  }
  
  if (requestType === 'batch') {
    stats.batchRequests++;
  } else {
    stats.singleRequests++;
  }
  
  // Update average response time
  if (stats.avgResponseTime === 0) {
    stats.avgResponseTime = responseTime;
  } else {
    stats.avgResponseTime = (stats.avgResponseTime * 0.9) + (responseTime * 0.1);
  }
}

// Log transaction system status
function logTxSystemStatus() {
  const stats = TX_SYSTEM.stats;
  const queueLength = txQueue.pending.length;
  
  // Initialize nonce tracking if needed
  if (!TX_SYSTEM.nonce) {
    TX_SYSTEM.nonce = {
      value: null,
      lastRefreshed: 0,
      pendingCount: 0,
      consecutiveErrors: 0,
      locallyIncremented: 0
    };
  }
  
  logger.info(`TX System Status: ${stats.totalSucceeded}/${stats.totalSubmitted} txs successful (${stats.batchRequests} batched, ${stats.singleRequests} single), ${queueLength} in queue`);
  logger.info(`Nonce status: current=${TX_SYSTEM.nonce.value}, locally incremented=${TX_SYSTEM.nonce.locallyIncremented}, consecutive errors=${TX_SYSTEM.nonce.consecutiveErrors}`);
  
  // Log each provider's stats
  for (let i = 0; i < RPC_CONFIG.providers.length; i++) {
    const provider = RPC_CONFIG.providers[i];
    if (!provider.enabled) continue;
    
    const providerStats = stats.providerStats[i];
    if (!providerStats) continue;
    
    const successRate = providerStats.totalSubmitted > 0 ? 
      (providerStats.totalSucceeded / providerStats.totalSubmitted * 100).toFixed(1) : 0;
    
    const backoffStatus = provider.rateLimit.inBackoff ? 
      `BACKOFF until ${new Date(provider.rateLimit.backoffUntil).toISOString()}` : 
      'ACTIVE';
    
    logger.info(`  Provider [${i}] ${provider.name}: ${successRate}% success, ` +
      `${providerStats.avgResponseTime.toFixed(0)}ms avg response, ${providerStats.nonceErrors || 0} nonce errors, ${backoffStatus}`);
  }
}

// Process the transaction queue
async function processTransactionQueue() {
  // Get transactions to process
  const queueLength = txQueue.pending.length;
  if (queueLength === 0) return;
  
  logger.debug(`Processing transaction queue (${queueLength} items)`);
  
  // Find the next available provider
  const providerIndex = getNextAvailableProvider();
  if (providerIndex === -1) {
    logger.warn(`No available providers to process transaction queue`);
    return;
  }
  
  const provider = RPC_CONFIG.providers[providerIndex];
  const web3 = web3Instances[providerIndex];
  
  // Initialize nonce tracking if not exists
  if (!TX_SYSTEM.nonce) {
    TX_SYSTEM.nonce = {
      value: null,
      lastRefreshed: 0,
      pendingCount: 0,
      consecutiveErrors: 0,
      locallyIncremented: 0
    };
  }
  
  // Check if we have a valid nonce
  if (TX_SYSTEM.nonce.value === null) {
    logger.warn(`Cannot process transactions: Nonce not yet initialized`);
    await refreshNonce(true);
    return;
  }
  
  // Get current nonce - if we've locally incremented too many times, refresh
  if (TX_SYSTEM.nonce.locallyIncremented >= 5) {
    logger.info(`Refreshing nonce after ${TX_SYSTEM.nonce.locallyIncremented} local increments`);
    await refreshNonce(true);
  }
  
  let currentNonce = TX_SYSTEM.nonce.value;
  
  // Get a batch of transactions to process
  const batchSize = Math.min(provider.maxBatchSize, txQueue.pending.length, TX_SYSTEM.config.batchSize);
  const batch = txQueue.getBatch(batchSize, providerIndex);
  
  if (batch.length === 0) return;
  
  logger.info(`Processing batch of ${batch.length} transactions using provider ${provider.name} (starting nonce: ${currentNonce})`);
  
  // Prepare and sign each transaction
  const signedTxs = [];
  const reportIds = [];
  
  for (const tx of batch) {
    try {
      // Add nonce to the transaction data
      const finalTx = {
        ...tx.txData,
        nonce: `0x${currentNonce.toString(16)}`
      };
      
      // Sign transaction
      const signedTx = await web3.eth.accounts.signTransaction(finalTx, account.privateKey);
      
      signedTxs.push(signedTx.rawTransaction);
      reportIds.push(tx.reportId);
      
      // Increment nonce for next transaction
      currentNonce = currentNonce + 1n;
      TX_SYSTEM.nonce.locallyIncremented++;
      
      logger.debug(`Prepared transaction for reportId=${tx.reportId} with nonce ${currentNonce - 1n}`);
    } catch (error) {
      logger.error(`Error preparing transaction for reportId=${tx.reportId}:`, error);
    }
  }
  
  // If no transactions were prepared, return
  if (signedTxs.length === 0) {
    logger.warn(`No transactions prepared for submission`);
    return;
  }
  
  // Increment pending tx count for this provider
  provider.pendingTxCount += signedTxs.length;
  
  // Submit the transactions
  let successfulTxs = [];
  
  try {
    // Try batch submission if supported or unknown
    if (provider.supportsBatching !== false) {
      successfulTxs = await batchSubmitTransactions(providerIndex, signedTxs, reportIds);
    } else {
      // Fall back to individual submission
      successfulTxs = await individualSubmitTransactions(providerIndex, signedTxs, reportIds);
    }
  } catch (error) {
    logger.error(`Error submitting transactions:`, error);
    
    // If we encounter an error that might be nonce-related, refresh nonce
    if (error.message && error.message.toLowerCase().includes('nonce')) {
      logger.warn(`Possible nonce error in batch submission, refreshing nonce`);
      await refreshNonce(true);
    }
  } finally {
    // Decrement pending tx count
    provider.pendingTxCount -= signedTxs.length;
  }
  
  // Remove successful transactions from the queue
  for (const reportId of successfulTxs) {
    txQueue.removeTransaction(reportId);
    
    // Mark as processed
    processedReports.add(Number(reportId));
    reportTimestamps.set(Number(reportId), Date.now());
  }
  
  logger.info(`Batch processing complete: ${successfulTxs.length}/${signedTxs.length} transactions successful`);
}

// Submit transactions using JSON-RPC batch request
async function batchSubmitTransactions(providerIndex, signedTxs, reportIds) {
  const provider = RPC_CONFIG.providers[providerIndex];
  
  try {
    // Create batch request
    const batchRequest = signedTxs.map((rawTx, i) => ({
      jsonrpc: '2.0',
      id: i + 1,
      method: 'eth_sendRawTransaction',
      params: [rawTx]
    }));
    
    logger.debug(`Submitting batch of ${signedTxs.length} transactions to ${provider.name}`);
    
    // Execute batch request
    const batchResponse = await executeBatchRequest(providerIndex, batchRequest);
    
    // Process responses
    const successfulReportIds = [];
    const nonceErrors = [];
    let otherErrors = 0;
    
    for (let i = 0; i < batchResponse.length; i++) {
      const response = batchResponse[i];
      const reportId = reportIds[i];
      
      if (response.error) {
        const errorMsg = response.error.message || '';
        
        // Track nonce errors separately
        if (errorMsg.toLowerCase().includes('nonce')) {
          logger.error(`Nonce error for reportId=${reportId}: ${errorMsg}`);
          nonceErrors.push({ reportId, error: errorMsg });
          TX_SYSTEM.stats.providerStats[providerIndex].nonceErrors++;
          TX_SYSTEM.nonce.consecutiveErrors++;
        } else {
          logger.error(`Error submitting transaction for reportId=${reportId}: ${errorMsg}`);
          otherErrors++;
        }
      } else if (response.result) {
        // Transaction submitted successfully
        logger.info(`Transaction submitted successfully for reportId=${reportId}, txHash=${response.result}`);
        successfulReportIds.push(reportId);
        
        // Update stats
        TX_SYSTEM.stats.totalSucceeded++;
        
        // Reset nonce errors
        TX_SYSTEM.nonce.consecutiveErrors = 0;
      }
    }
    
    // If we had any nonce errors, force a nonce refresh
    if (nonceErrors.length > 0) {
      logger.warn(`Encountered ${nonceErrors.length} nonce errors, forcing nonce refresh`);
      await refreshNonce(true);
    }
    
    // Update stats
    TX_SYSTEM.stats.batchRequests++;
    TX_SYSTEM.stats.totalSubmitted += signedTxs.length;
    
    return successfulReportIds;
  } catch (error) {
    logger.error(`Error in batch transaction submission:`, error);
    
    // If batching fails due to provider not supporting it, try individual submission
    if (provider.supportsBatching === null) {
      provider.supportsBatching = false;
      logger.warn(`Provider ${provider.name} does not support batching, falling back to individual submission`);
      return individualSubmitTransactions(providerIndex, signedTxs, reportIds);
    }
    
    return [];
  }
}

// Submit transactions individually
async function individualSubmitTransactions(providerIndex, signedTxs, reportIds) {
  const provider = RPC_CONFIG.providers[providerIndex];
  const web3 = web3Instances[providerIndex];
  const successfulReportIds = [];
  const nonceErrors = [];
  
  logger.debug(`Submitting ${signedTxs.length} transactions individually to ${provider.name}`);
  
  for (let i = 0; i < signedTxs.length; i++) {
    const rawTx = signedTxs[i];
    const reportId = reportIds[i];
    
    try {
      // Submit transaction
      const startTime = Date.now();
      const receipt = await web3.eth.sendSignedTransaction(rawTx);
      const responseTime = Date.now() - startTime;
      
      // Update stats
      updateProviderStats(providerIndex, 'single', true, responseTime);
      TX_SYSTEM.stats.totalSucceeded++;
      TX_SYSTEM.stats.singleRequests++;
      
      // Reset nonce errors
      TX_SYSTEM.nonce.consecutiveErrors = 0;
      
      // Reset backoff
      resetProviderBackoff(providerIndex);
      
      logger.info(`Transaction successful for reportId=${reportId}, txHash=${receipt.transactionHash}`);
      successfulReportIds.push(reportId);
    } catch (error) {
      // Update stats
      updateProviderStats(providerIndex, 'single', false, 0);
      TX_SYSTEM.stats.totalFailed++;
      TX_SYSTEM.stats.singleRequests++;
      
      // Handle nonce errors specifically
      if (error.message && error.message.toLowerCase().includes('nonce')) {
        logger.error(`Nonce error for reportId=${reportId}: ${error.message}`);
        TX_SYSTEM.stats.providerStats[providerIndex].nonceErrors++;
        TX_SYSTEM.nonce.consecutiveErrors++;
        nonceErrors.push({ reportId, error: error.message });
      } else {
        logger.error(`Error submitting transaction for reportId=${reportId}:`, error);
      }
      
      // Handle rate limiting
      if (isRateLimitError(error)) {
        applyProviderBackoff(providerIndex);
        break; // Stop processing more transactions with this provider
      }
    }
    
    // Add a small delay between individual submissions to avoid overwhelming the provider
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  // If we had any nonce errors, force a nonce refresh
  if (nonceErrors.length > 0) {
    logger.warn(`Encountered ${nonceErrors.length} nonce errors, forcing nonce refresh`);
    await refreshNonce(true);
  }
  
  return successfulReportIds;
}

// Build and queue a dispute transaction
async function buildAndQueueDisputeTx(reportId, disputeParams, timeRemaining) {
  const { tokenToSwap, newAmount1, newAmount2 } = disputeParams;
  
  try {
    // Get gas estimate from any available provider
    const providerIndex = getNextAvailableProvider();
    if (providerIndex === -1) {
      logger.warn(`Cannot build transaction: No available providers`);
      return false;
    }
    
    const web3 = web3Instances[providerIndex];
    
    // Build transaction data
    const disputeData = web3.eth.abi.encodeFunctionCall({
      name: 'disputeAndSwap',
      type: 'function',
      inputs: [
        { type: 'uint256', name: 'reportId' },
        { type: 'address', name: 'tokenToSwap' },
        { type: 'uint256', name: 'newAmount1' },
        { type: 'uint256', name: 'newAmount2' }
      ]
    }, [reportId.toString(), tokenToSwap, newAmount1.toString(), newAmount2.toString()]);
    
    // Estimate gas - This will fail with "Dispute too early" if dispute delay hasn't passed
    let gasEstimate;
    try {
      gasEstimate = await web3.eth.estimateGas({
        from: account.address,
        to: openOracleAddress,
        data: disputeData
      });
      logger.debug(`Gas estimate for report ${reportId}: ${gasEstimate}`);
    } catch (error) {
      // Check if this is a "Dispute too early" error
      if (error.message && error.message.includes('Dispute too early')) {
        // Get current timestamp and report data to calculate when we can retry
        try {
          const status = await web3.eth.getContract(openOracleAbi, openOracleAddress)
                                    .methods.reportStatus(reportId).call();
          const meta = await web3.eth.getContract(openOracleAbi, openOracleAddress)
                                  .methods.reportMeta(reportId).call();
          
          const reportTimestamp = Number(status.reportTimestamp);
          const disputeDelay = Number(meta.disputeDelay);
          const now = Math.floor(Date.now() / 1000);
          
          const timeUntilOpen = (reportTimestamp + disputeDelay) - now;
          if (timeUntilOpen > 0) {
            logger.info(`Dispute window not yet open for reportId=${reportId}, need to wait ${timeUntilOpen} more seconds`);
            const delayMs = (timeUntilOpen + 1) * 1000; // +1 second buffer
            queueReportForRetry(reportId, { disputeParams, timeRemaining }, delayMs);
            return true; // Return true because we've scheduled a retry
          }
        } catch (fetchError) {
          logger.error(`Error fetching report data for retry calculation:`, fetchError);
        }
      }
      
      logger.error(`Error estimating gas for reportId=${reportId}:`, error);
      return false;
    }
    
    // Get current gas price info
    let maxFeePerGas, maxPriorityFeePerGas;
    try {
      // Try to get EIP-1559 parameters
      const block = await web3.eth.getBlock('latest');
      
      if (block.baseFeePerGas) {
        // EIP-1559 supported
        const baseFee = BigInt(block.baseFeePerGas);
        
        // Set max priority fee to 10% of base fee with minimum
        const priorityFee = baseFee / 10n > MIN_PRIORITY_FEE_WEI ? baseFee / 10n : MIN_PRIORITY_FEE_WEI;
        
        // Set max fee to base fee + 20% buffer + priority fee
        maxFeePerGas = baseFee + (baseFee / 5n) + priorityFee;
        maxPriorityFeePerGas = priorityFee;
        
        logger.debug(`Using EIP-1559 gas: maxFee=${formatWei(maxFeePerGas).gwei} gwei, maxPriority=${formatWei(maxPriorityFeePerGas).gwei} gwei`);
      } else {
        // Legacy gas price
        const gasPrice = BigInt(await web3.eth.getGasPrice());
        maxFeePerGas = gasPrice;
        maxPriorityFeePerGas = gasPrice / 10n > MIN_PRIORITY_FEE_WEI ? gasPrice / 10n : MIN_PRIORITY_FEE_WEI;
        
        logger.debug(`Using legacy gas price: ${formatWei(gasPrice).gwei} gwei`);
      }
    } catch (error) {
      logger.error(`Error getting gas price for reportId=${reportId}:`, error);
      return false;
    }
    
    // Build transaction object
    const txData = {
      from: account.address,
      to: openOracleAddress,
      data: disputeData,
      gas: Math.floor(Number(gasEstimate) * 1.2), // Add 20% gas buffer
      maxFeePerGas: `0x${maxFeePerGas.toString(16)}`,
      maxPriorityFeePerGas: `0x${maxPriorityFeePerGas.toString(16)}`,
      type: '0x2' // EIP-1559 transaction
    };
    
    // Add to transaction queue
    txQueue.addTransaction(reportId, txData, timeRemaining);
    
    return true;
  } catch (error) {
    logger.error(`Error building transaction for reportId=${reportId}:`, error);
    return false;
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
    logger.debug(`Rate limiting Kraken API, waiting ${waitTime}ms`);
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
    logger.error(`Error fetching Kraken mid for ${pairName}:`, error);
    throw error;
  }
}

// Get prices from cache, verifying they're not too old
function getPriceData() {
  const now = Date.now();
  const cacheAge = now - priceCache.lastUpdate;
  
  // Check if we have valid price data
  if (priceCache.ethMid === null || priceCache.usdcMid === null) {
    return null;
  }
  
  // Check if the cache is too old 
  if (cacheAge > CONFIG.MAX_PRICE_CACHE_AGE_MS) {
    logger.warn(`Price data is too old (${cacheAge}ms > ${CONFIG.MAX_PRICE_CACHE_AGE_MS}ms maximum age)`);
    return null;
  }
  
  // Log cache status
  if (cacheAge > priceCache.CACHE_TTL_MS) {
    logger.info(`Using stale price data: ETH/USD=${priceCache.ethMid}, USDC/USD=${priceCache.usdcMid} (cache age: ${cacheAge}ms)`);
  } else {
    logger.debug(`Using fresh price data: ETH/USD=${priceCache.ethMid}, USDC/USD=${priceCache.usdcMid} (cache age: ${cacheAge}ms)`);
  }
  
  return { 
    ethMid: priceCache.ethMid, 
    usdcMid: priceCache.usdcMid,
    cacheAge
  };
}

// Modified version of getBothMid that uses the regularly updated cache
async function getBothMid() {
  // First check if we have valid recent cache data
  const priceData = getPriceData();
  if (priceData !== null) {
    return priceData;
  }
  
  // If cache is too old or invalid, try to update it now
  logger.info(`No valid price data in cache, fetching fresh data`);
  
  try {
    // Rate limiting enforcement
    const now = Date.now();
    const timeElapsed = now - krakenRateLimiter.lastCalled;
    if (timeElapsed < krakenRateLimiter.MIN_INTERVAL_MS) {
      const waitTime = krakenRateLimiter.MIN_INTERVAL_MS - timeElapsed;
      logger.debug(`Rate limiting Kraken API, waiting ${waitTime}ms`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    
    krakenRateLimiter.lastCalled = now;

    const [ethMid, usdcMid] = await Promise.all([
      getKrakenMid('ETH/USD'),
      getKrakenMid('USDC/USD')
    ]);
    
    // Using fixed cache TTL, no need to adjust based on volatility
    
    // Update cache with fresh data
    priceCache.lastUpdate = now;
    priceCache.ethMid = ethMid;
    priceCache.usdcMid = usdcMid;
    priceCache.usingExtendedCache = false;
    
    logger.info(`Fresh price data: ETH/USD=${ethMid}, USDC/USD=${usdcMid}`);
    return { ethMid, usdcMid, cacheAge: 0 };
  } catch (error) {
    // Check for rate limiting
    if (isRateLimitError(error)) {
      increaseBackoff();
      logger.warn(`Rate limit encountered while fetching prices: ${error.message}`);
    }
    
    // Throw the error up - we have no valid data to return
    throw error;
  }
}

// Dedicated function to update price cache on a regular schedule
async function updatePriceCache() {
  // Don't overlap updates
  if (priceCache.updateInProgress) {
    logger.debug(`Price update already in progress, skipping this cycle`);
    return;
  }
  
  // Flag that we're updating
  priceCache.updateInProgress = true;
  
  try {
    // Check if we're in backoff mode
    if (priceCache.inBackoffMode) {
      const backoffEndTime = priceCache.lastUpdate + (currentPollInterval - CONFIG.PRICE_UPDATE_INTERVAL_MS);
      if (Date.now() < backoffEndTime) {
        logger.debug(`Still in price update backoff mode, skipping update`);
        return;
      } else {
        // Backoff period is over
        logger.info(`Price update backoff period has ended, resuming normal updates`);
        priceCache.inBackoffMode = false;
      }
    }
    
    // Fetch latest prices
    logger.debug(`Fetching fresh price data from Kraken`);
    
    // Rate limiting enforcement for Kraken API
    const now = Date.now();
    const timeElapsed = now - krakenRateLimiter.lastCalled;
    if (timeElapsed < krakenRateLimiter.MIN_INTERVAL_MS) {
      const waitTime = krakenRateLimiter.MIN_INTERVAL_MS - timeElapsed;
      logger.debug(`Rate limiting Kraken API, waiting ${waitTime}ms`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    
    // Update last call time
    krakenRateLimiter.lastCalled = Date.now();
    
    // Fetch ETH/USD and USDC/USD prices in parallel
    const [ethMid, usdcMid] = await Promise.all([
      getKrakenMid('ETH/USD'),
      getKrakenMid('USDC/USD')
    ]);
    
    // Fixed cache TTL - no need to update based on volatility
    
    // Update cache with fresh data
    priceCache.lastUpdate = Date.now();
    priceCache.ethMid = ethMid;
    priceCache.usdcMid = usdcMid;
    priceCache.usingExtendedCache = false;
    priceCache.lastError = null;
    
    logger.info(`Price cache updated: ETH/USD=${ethMid}, USDC/USD=${usdcMid}`);
    
    // Successfully updated, reduce backoff if needed
    decreaseBackoff();
    
  } catch (error) {
    logger.error(`Failed to update price cache: ${error.message}`);
    priceCache.lastError = error.message;
    
    // Handle rate limiting
    if (isRateLimitError(error)) {
      logger.warn(`Rate limit hit while updating prices, applying backoff`);
      increaseBackoff();
      priceCache.inBackoffMode = true;
    }
  } finally {
    // Clear the update flag
    priceCache.updateInProgress = false;
  }
}

// Function removed - using fixed cache TTL instead of volatility-based adaptive TTL

/************************************************************
 *   Process a report with provided price data (modified to use multi-provider system)
 ************************************************************/
async function processReportWithPrices(reportId, eventData, priceData) {
  const idNum = Number(reportId);
  
  try {
    // Skip if we've already processed or are in the process of processing this report
    if (processedReports.has(idNum)) {
      logger.info(`Skipping, already processed reportId=${idNum}`);
      return;
    }
    
    if (pendingDisputes.has(idNum)) {
      logger.info(`Skipping, transaction is pending for reportId=${idNum}`);
      return;
    }
    
    // Double-check if this report is in the retry queue
    if (rateLimitedReports.has(idNum)) {
      const retryInfo = rateLimitedReports.get(idNum);
      const now = Date.now();
      if (now < retryInfo.nextRetryTime) {
        logger.info(`Skipping reportId=${idNum}, scheduled for retry at ${new Date(retryInfo.nextRetryTime).toISOString()}`);
        return;
      }
    }
    
    // Track this report as being processed
    pendingDisputes.set(idNum, Date.now());
    logger.info(`Processing reportId=${idNum} with provided price data, locked for dispute processing`);
    
    // Get report status and metadata with rate limit handling
    let status, meta;
    try {
      // Use the first available provider
      const providerIndex = getNextAvailableProvider();
      if (providerIndex === -1) {
        logger.warn(`No available providers to process report ${idNum}`);
        queueReportForRetry(idNum, eventData);
        return;
      }
      
      const web3 = web3Instances[providerIndex];
      const contract = new web3.eth.Contract(openOracleAbi, openOracleAddress);
      
      status = await contract.methods.reportStatus(idNum).call();
      
      // Skip if already settled or distributed
      if (status.isSettled || status.isDistributed) {
        logger.info(`Skipping settled or distributed reportId=${idNum}`);
        processedReports.add(idNum);
        reportTimestamps.set(idNum, Date.now());
        pendingDisputes.delete(idNum);
        return;
      }
      
      meta = await contract.methods.reportMeta(idNum).call();
    } catch (error) {
      logger.error(`Error fetching report data for reportId=${idNum}:`, error);
      if (isRateLimitError(error)) {
        // Queue for retry if rate limited
        queueReportForRetry(idNum, eventData);
        return;
      }
      pendingDisputes.delete(idNum);
      return;
    }
    
    const token1 = meta.token1.toLowerCase();
    const token2 = meta.token2.toLowerCase();
    
    // Only handle WETH/USDC pairs
    if (token1 !== '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' || 
        token2 !== '0xaf88d065e77c8cc2239327c5edb3a432268e5831') {
      logger.info(`Skipping non-WETH/USDC reportId=${idNum}`);
      processedReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Use the provided price data instead of fetching
    const ethMid = priceData.ethMid;
    const usdcMid = priceData.usdcMid;
    logger.info(`Using provided price data: ETH/USD=${ethMid}, USDC/USD=${usdcMid}`);
    
    // Calculate USD values for the tokens
    const token1Usd = (Number(status.currentAmount1) / 1e18) * ethMid;
    const token2Usd = (Number(status.currentAmount2) / 1e6) * usdcMid;
    const usdDiff = Math.abs(token1Usd - token2Usd);
    
    logger.info(`Report ${idNum} values:`, {
      token1: `${Number(status.currentAmount1) / 1e18} WETH (${token1Usd.toFixed(2)})`,
      token2: `${Number(status.currentAmount2) / 1e6} USDC (${token2Usd.toFixed(2)})`,
      usdDiff: `${usdDiff.toFixed(2)}`
    });
    
    // Check if the time window for disputes has closed
    const now = Math.floor(Date.now() / 1000);
    const reportTimestamp = Number(status.reportTimestamp);
    const settlementTime = Number(meta.settlementTime);
    const disputeDelay = Number(meta.disputeDelay);
    
    if (now > reportTimestamp + settlementTime) {
      logger.info(`Skipping reportId=${idNum}, dispute window closed`);
      processedReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Check if we're still within the dispute delay
    if (now < reportTimestamp + disputeDelay) {
      const timeUntilOpen = (reportTimestamp + disputeDelay) - now;
      logger.info(`Dispute window not yet open for reportId=${idNum}, need to wait ${timeUntilOpen} more seconds`);
      
      // Don't even attempt to estimate gas or build transactions yet
      // Calculate exact milliseconds until the window opens plus a small buffer
      const delayMs = (timeUntilOpen + 1) * 1000; // +1 second buffer
      logger.info(`Scheduling retry in ${delayMs}ms when dispute window opens`);
      
      // Schedule for retry with absolute delay time (not a timestamp)
      queueReportForRetry(idNum, eventData, delayMs);
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Calculate time remaining until settlement closes
    const timeRemaining = (reportTimestamp + settlementTime) - now;
    
    // Determine which token to swap and calculate new amounts
    let tokenToSwap, newAmount1, newAmount2, totalCommitUsd;
    if (token1Usd < token2Usd) {
      tokenToSwap = token1;
      newAmount1 = (BigInt(status.currentAmount1) * BigInt(meta.multiplier)) / 100n;
      newAmount2 = BigInt(Math.floor((Number(newAmount1) / 1e18) * ethMid / usdcMid * 1e6));
      totalCommitUsd = token1Usd + (Number(meta.multiplier) / 100) * token1Usd + ((Number(meta.multiplier) / 100) - 1) * token2Usd;
    } else {
      tokenToSwap = token2;
      newAmount1 = (BigInt(status.currentAmount1) * BigInt(meta.multiplier)) / 100n;
      newAmount2 = BigInt(Math.floor((Number(newAmount1) / 1e18) * ethMid / usdcMid * 1e6));
      totalCommitUsd = token2Usd + (Number(meta.multiplier) / 100) * token2Usd + ((Number(meta.multiplier) / 100) - 1) * token1Usd;
    }
    
    logger.info(`Dispute parameters for reportId=${idNum}:`, {
      tokenToSwap: tokenToSwap === token1 ? 'WETH' : 'USDC',
      newAmount1: `${Number(newAmount1) / 1e18} WETH`,
      newAmount2: `${Number(newAmount2) / 1e6} USDC`,
      totalCommitUsd: `${totalCommitUsd.toFixed(2)}`
    });
    
    // Do price deviation check (will the new price be different enough to pass the fee boundary?)
    const oldPrice = (BigInt(status.currentAmount1) * 10n**18n) / BigInt(status.currentAmount2);
    const newPrice = (newAmount1 * 10n**18n) / newAmount2;
    const feeBoundaryBps = BigInt(meta.feePercentage);
    const feeBoundary = (oldPrice * feeBoundaryBps) / 10n**7n;
    const lowerBoundary = oldPrice > feeBoundary ? oldPrice - feeBoundary : 0n;
    const upperBoundary = oldPrice + feeBoundary;
    
    logger.info(`Price boundary check:`, {
      oldPrice: Number(oldPrice) / 1e18,
      newPrice: Number(newPrice) / 1e18,
      lowerBoundary: Number(lowerBoundary) / 1e18,
      upperBoundary: Number(upperBoundary) / 1e18,
      feeBoundaryBps: Number(feeBoundaryBps) / 1e5 // convert to basis points for readability
    });
    
    if (newPrice >= lowerBoundary && newPrice <= upperBoundary) {
      logger.info(`SKIPPING: New price (${Number(newPrice) / 1e18}) is within fee boundaries`);
      processedReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Get gas price for profitability calculation
    const providerIndex = getNextAvailableProvider();
    if (providerIndex === -1) {
      logger.warn(`No available providers to get gas price for report ${idNum}`);
      queueReportForRetry(idNum, eventData);
      return;
    }
    
    const web3 = web3Instances[providerIndex];
    let gasPriceWei;
    
    try {
      gasPriceWei = await web3.eth.getGasPrice();
      logger.debug(`Current gas price: ${formatWei(BigInt(gasPriceWei)).gwei} gwei`);
    } catch (error) {
      logger.error(`Error getting gas price for reportId=${idNum}:`, error);
      if (isRateLimitError(error)) {
        queueReportForRetry(idNum, eventData);
        return;
      }
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Calculate gas cost and profitability (use a conservative gas estimate)
    const gasEstimate = 300000; // Conservative estimate
    const gasCostEth = (Number(gasEstimate) * Number(gasPriceWei)) / 1e18;
    const gasCostUsd = gasCostEth * ethMid;
    const swapFeeUsd = (Number(meta.feePercentage) / 1e7) * totalCommitUsd;
    const protocolFeeUsd = (Number(meta.protocolFee) / 1e7) * totalCommitUsd;
    
    const netProfitability = (usdDiff - swapFeeUsd - protocolFeeUsd - gasCostUsd) / totalCommitUsd;
    
    // Enhanced logging for profitability analysis
    logger.info(`Profitability analysis for reportId=${idNum}:`);
    logger.info(`  Token1 USD: $${token1Usd.toFixed(2)}, Token2 USD: $${token2Usd.toFixed(2)}`);
    logger.info(`  USD Diff: $${usdDiff.toFixed(2)}, Total Commit USD: $${totalCommitUsd.toFixed(2)}`);
    logger.info(`  Swap Fee: $${swapFeeUsd.toFixed(2)}, Protocol Fee: $${protocolFeeUsd.toFixed(2)}`);
    logger.info(`  Est. Gas Cost: $${gasCostUsd.toFixed(2)} (${gasCostEth.toFixed(6)} ETH at $${ethMid.toFixed(2)})`);
    logger.info(`  Est. Net Profit: $${(usdDiff - swapFeeUsd - protocolFeeUsd - gasCostUsd).toFixed(2)}`);
    logger.info(`  Profitability: ${(netProfitability * 100).toFixed(4)}%`);
    
    // Check if profitable enough to proceed
    if (netProfitability < MIN_PROFITABILITY_THRESHOLD) {
      logger.info(`UNPROFITABLE: Skipping reportId=${idNum}, profitability ${(netProfitability * 100).toFixed(4)}% below threshold ${(MIN_PROFITABILITY_THRESHOLD * 100).toFixed(4)}%`);
      processedReports.add(idNum);
      reportTimestamps.set(idNum, Date.now());
      pendingDisputes.delete(idNum);
      return;
    }
    
    // Build and queue transaction with improved priority
    logger.info(`Dispute for reportId=${idNum} is profitable, building and queueing transaction with time remaining: ${timeRemaining}s`);
    const disputeParams = {
      tokenToSwap,
      newAmount1,
      newAmount2
    };
    
    const success = await buildAndQueueDisputeTx(idNum, disputeParams, timeRemaining);
    
    if (success) {
      logger.info(`Transaction queued successfully for reportId=${idNum}`);
    } else {
      logger.error(`Failed to queue transaction for reportId=${idNum}`);
      pendingDisputes.delete(idNum);
    }
  } catch (error) {
    logger.error(`Error processing reportId=${idNum}:`, error);
    
    // Check if this was a rate limit error
    if (isRateLimitError(error)) {
      queueReportForRetry(idNum, eventData);
      return;
    }
    
    pendingDisputes.delete(idNum);
  }
}

/************************************************************
 *   Process a single report (with retry support)
 ************************************************************/
async function processReport(reportId, eventData) {
  // Get price data
  try {
    const priceData = await getBothMid();
    await processReportWithPrices(reportId, eventData, priceData);
  } catch (error) {
    logger.error(`Error getting price data for reportId=${reportId}:`, error);
    
    // Check if this was a rate limit error
    if (isRateLimitError(error)) {
      queueReportForRetry(reportId, eventData);
    } else {
      pendingDisputes.delete(Number(reportId));
    }
  }
}

/************************************************************
 *   MEMORY MANAGEMENT: Purge old data to prevent leaks
 ************************************************************/
function purgeOldData() {
  const now = Date.now();
  logger.info(`Running memory cleanup...`);
  
  // 1. Clean up old pending transactions
  let pendingPurgeCount = 0;
  for (const [reportId, timestamp] of pendingDisputes.entries()) {
    if (now - timestamp > DISPUTE_LOCK_EXPIRY_MS) {
      pendingDisputes.delete(reportId);
      pendingPurgeCount++;
    }
  }
  
  // 2. Clean up oldest processed reports if we have too many
  if (processedReports.size > MAX_PROCESSED_REPORTS) {
    // Sort by timestamp (oldest first)
    const sortedReportIds = [...reportTimestamps.entries()]
      .sort((a, b) => a[1] - b[1])
      .map(entry => entry[0]);
    
    // Calculate how many to remove
    const reportsToRemove = processedReports.size - MAX_PROCESSED_REPORTS;
    
    // Remove oldest reports
    for (let i = 0; i < reportsToRemove && i < sortedReportIds.length; i++) {
      const oldReportId = sortedReportIds[i];
      processedReports.delete(oldReportId);
      reportTimestamps.delete(oldReportId);
    }
    
    logger.info(`Removed ${reportsToRemove} oldest processed reports from memory`);
  }
  
  // 3. Clean up old reports by age
  let agePurgeCount = 0;
  for (const [reportId, timestamp] of reportTimestamps.entries()) {
    if (now - timestamp > REPORT_AGE_THRESHOLD_MS) {
      processedReports.delete(reportId);
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
  
  // 5. Clean transaction queue
  const txQueuePurged = txQueue.cleanQueue();
  
  // Log cleanup results
  logger.info(`Memory cleanup complete: Removed ${pendingPurgeCount} stale pending transactions, ${agePurgeCount} old reports, ${retryPurgeCount} stale retry reports, and ${txQueuePurged} old queued transactions`);
  logger.info(`Current memory usage: ${processedReports.size} processed reports, ${pendingDisputes.size} pending disputes, ${rateLimitedReports.size} queued retries, ${txQueue.pending.length} transactions in queue`);
  
  // Force garbage collection if available (Node.js with --expose-gc flag)
  if (global.gc) {
    logger.info(`Triggering garbage collection...`);
    global.gc();
  }
}

/************************************************************
 *   MAIN LOGIC: checkForReportsToDispute
 ************************************************************/
async function checkForReportsToDispute() {
  try {
    // Cleanup expired transaction locks
    cleanupExpiredTransactions();
    
    // Clean up stale settlement info periodically
    reportSettlementInfo.cleanupOldReports();
    
    // Process reports that were previously rate-limited and are ready for retry
    await processRateLimitedReports();
    
    // Process a batch from the event queue
    if (eventQueue.pendingReports.length > 0) {
      logger.info(`Processing batch of events from queue (${eventQueue.pendingReports.length} pending reports)`);
      const BATCH_SIZE = 10; // Process up to 10 reports at once
      await eventQueue.processBatch(BATCH_SIZE);
    }
    
    // Get latest block using the active provider
    const providerIndex = getNextAvailableProvider();
    if (providerIndex === -1) {
      logger.warn(`No available providers to check for new blocks`);
      return;
    }
    
    let latestBlock;
    try {
      latestBlock = Number(await web3Instances[providerIndex].eth.getBlockNumber());
      
      // Reset backoff for this provider
      resetProviderBackoff(providerIndex);
    } catch (error) {
      logger.error(`Error getting latest block:`, error);
      if (isRateLimitError(error)) {
        increaseBackoff();
        applyProviderBackoff(providerIndex);
      }
      return;
    }
    
    if (lastBlockChecked === 0) {
      lastBlockChecked = latestBlock - 50; // Last 50 blocks on first run
      logger.info(`First run, starting from block ${lastBlockChecked}`);
      return;
    }
    
    if (latestBlock < lastBlockChecked) {
      logger.info(`Latest block (${latestBlock}) < lastBlockChecked (${lastBlockChecked}), skipping...`);
      return;
    }

    // Limit the block range to avoid processing too many blocks at once
    const MAX_BLOCKS_PER_QUERY = 50;
    const fromBlock = lastBlockChecked + 1;
    const toBlock = Math.min(latestBlock, fromBlock + MAX_BLOCKS_PER_QUERY - 1);
    
    logger.info(`Checking for events from block ${fromBlock} to ${toBlock}`);
    
    // Get events with potential rate limit handling
    let events;
    try {
      const web3 = web3Instances[providerIndex];
      const contract = new web3.eth.Contract(openOracleAbi, openOracleAddress);
      
      events = await contract.getPastEvents('allEvents', {
        fromBlock,
        toBlock
      });
      
      // Only update lastBlockChecked if successfully got events
      lastBlockChecked = toBlock;
      
      // If we successfully got events, we can reduce backoff
      decreaseBackoff();
      resetProviderBackoff(providerIndex);
    } catch (error) {
      logger.error(`Error getting past events: ${error.message}`);
      
      // Check if this is a rate limit error
      if (isRateLimitError(error)) {
        // Apply exponential backoff
        increaseBackoff();
        applyProviderBackoff(providerIndex);
      }
      
      // Don't update lastBlockChecked to ensure we retry these blocks
      return;
    }
    
    logger.info(`Found ${events.length} total events`);
    
    // First process ReportInstanceCreated events to build our settlement info database
    const creationEvents = events.filter(evt => evt.event === 'ReportInstanceCreated');
    if (creationEvents.length > 0) {
      logger.info(`Processing ${creationEvents.length} report creation events`);
      
      for (const evt of creationEvents) {
        const reportId = Number(evt.returnValues.reportId);
        const settlementTime = Number(evt.returnValues.settlementTime);
        const escalationHalt = Number(evt.returnValues.escalationHalt);
        const disputeDelay = Number(evt.returnValues.disputeDelay);
        
        // Store this info for future reference
        reportSettlementInfo.addReport(reportId, {
          settlementTime,
          escalationHalt,
          disputeDelay
        });
        
        logger.info(`Recorded settlement info for reportId=${reportId}: settlementTime=${settlementTime}s, disputeDelay=${disputeDelay}s`);
      }
    }
    
    // Filter for actionable events (initial reports and disputes)
    const actionableEvents = events.filter(evt => 
      ['InitialReportSubmitted', 'ReportDisputed'].includes(evt.event)
    );
    logger.info(`Found ${actionableEvents.length} actionable events`);
    
    // Process settled events to avoid trying to dispute already settled reports
    const settledEvents = events.filter(evt => evt.event === 'ReportSettled');
    for (const evt of settledEvents) {
      const reportId = Number(evt.returnValues.reportId);
      logger.info(`Marking reportId=${reportId} as settled (from event)`);
      processedReports.add(reportId);
      reportTimestamps.set(reportId, Date.now());
    }
    
    // Add actionable events to processing queue with time remaining info
    let queuedCount = 0;
    for (const evt of actionableEvents) {
      const reportId = Number(evt.returnValues.reportId);
      
      // Skip already processed reports
      if (processedReports.has(reportId) || pendingDisputes.has(reportId)) {
        continue;
      }
      
      // Get report status to determine current report timestamp
      let reportTimestamp = null;
      try {
        const providerIdx = getNextAvailableProvider();
        if (providerIdx === -1) {
          logger.warn(`No available providers to get report status for ${reportId}`);
          continue;
        }
        
        const web3 = web3Instances[providerIdx];
        const contract = new web3.eth.Contract(openOracleAbi, openOracleAddress);
        
        const status = await contract.methods.reportStatus(reportId).call();
        reportTimestamp = Number(status.reportTimestamp);
      } catch (error) {
        logger.error(`Error getting report status for reportId=${reportId}:`, error);
        if (!isRateLimitError(error)) continue;
      }
      
      // Add to queue with report timestamp for time remaining calculation
      if (eventQueue.addEvent(reportId, evt, reportTimestamp)) {
        queuedCount++;
      }
    }
    
    logger.info(`Added ${queuedCount} new events to processing queue`);
    
    // Clean any old events from the queue
    eventQueue.cleanQueue();
    
    // Process a batch immediately if we have new events
    if (queuedCount > 0) {
      const INITIAL_BATCH_SIZE = 5; // Process up to 5 reports on first pass
      await eventQueue.processBatch(INITIAL_BATCH_SIZE);
    }
    
    logger.info(`Successfully processed blocks ${fromBlock} to ${toBlock}`);
  } catch (err) {
    logger.error('Error in checkForReportsToDispute:', err);
    
    // Check if this is a rate limit error
    if (isRateLimitError(err)) {
      // Apply exponential backoff
      increaseBackoff();
    }
    
    // Don't update lastBlockChecked on error to ensure no blocks are missed
  }
}

/************************************************************
 *   CLEANUP EXPIRED TRANSACTIONS
 ************************************************************/
function cleanupExpiredTransactions() {
  const now = Date.now();
  for (const [reportId, timestamp] of pendingDisputes.entries()) {
    if (now - timestamp > DISPUTE_LOCK_EXPIRY_MS) {
      pendingDisputes.delete(reportId);
      logger.debug(`Removed expired dispute lock for reportId=${reportId}`);
    }
  }
}

/************************************************************
 *   HELPER FUNCTION: Decode Transaction Error
 ************************************************************/
async function decodeTransactionError(txHash) {
  try {
    // Find an available provider
    const providerIndex = getNextAvailableProvider();
    if (providerIndex === -1) {
      return { error: 'No available providers to decode transaction error' };
    }
    
    const web3 = web3Instances[providerIndex];
    
    // Get the transaction data
    const tx = await web3.eth.getTransaction(txHash);
    if (!tx) {
      return { error: 'Transaction not found' };
    }
    
    // Try to get the transaction receipt
    const receipt = await web3.eth.getTransactionReceipt(txHash);
    if (!receipt) {
      return { error: 'Transaction receipt not found' };
    }
    
    // If the transaction was successful, there's no error to decode
    if (receipt.status) {
      return { success: true };
    }
    
    // Attempt to trace the transaction (this only works with certain RPC providers that support tracing)
    try {
      const trace = await web3.eth.call({
        to: tx.to,
        from: tx.from,
        data: tx.input,
        value: tx.value,
        gas: tx.gas,
        gasPrice: tx.gasPrice
      }, receipt.blockNumber);
      
      // If we get here, the transaction actually succeeded when replayed
      return { 
        success: false, 
        status: receipt.status,
        trace: 'Trace succeeded but transaction failed' 
      };
    } catch (traceError) {
      // This is expected - we want to extract the revert reason from the error
      let revertReason = 'Unknown revert reason';
      
      if (traceError.message) {
        // Different providers format this differently
        const message = traceError.message;
        
        // Look for revert reason in various formats
        if (message.includes('reverted with reason string')) {
          const match = message.match(/'(.+?)'/);
          if (match && match[1]) {
            revertReason = match[1];
          }
        } else if (message.includes('revert')) {
          revertReason = message;
        }
      }
      
      return {
        success: false,
        revertReason,
        status: receipt.status,
        gasUsed: receipt.gasUsed
      };
    }
  } catch (error) {
    return { error: `Error decoding transaction: ${error.message}` };
  }
}

/************************************************************
 *   CREATOR FILTER FUNCTION
 ************************************************************/
async function checkForCreatorMatch(reportId) {
  // If creator filter mode is disabled, accept all creators
  if (!CONFIG.CREATOR_FILTER_ENABLED) {
    return true;
  }
  
  // If filter is enabled but empty, accept no creators
  if (!CONFIG.CREATOR_FILTER || CONFIG.CREATOR_FILTER.length === 0) {
    logger.warn(`Creator filter is enabled but no creators are specified. Skipping reportId=${reportId}`);
    return false;
  }
  
  try {
    // Find an available provider
    const providerIndex = getNextAvailableProvider();
    if (providerIndex === -1) {
      logger.warn(`No available providers to check creator match for ${reportId}`);
      return false;
    }
    
    const web3 = web3Instances[providerIndex];
    const contract = new web3.eth.Contract(openOracleAbi, openOracleAddress);
    
    // We need to find the creation event for this report to check the creator
    const fromBlock = 0; // Start from the beginning or use a known starting block
    const events = await contract.getPastEvents('ReportInstanceCreated', {
      filter: { reportId: reportId },
      fromBlock
    });
    
    // If no creation event found, we can't verify the creator
    if (events.length === 0) {
      logger.warn(`No creation event found for reportId=${reportId}, can't verify creator`);
      return false;
    }
    
    // Check if the creator matches any in our filter list
    const creator = events[0].returnValues.creator.toLowerCase();
    const matches = CONFIG.CREATOR_FILTER.some(address => 
      address.toLowerCase() === creator
    );
    
    if (matches) {
      logger.info(`Report creator ${creator} is in the allowed list for reportId=${reportId}`);
    } else {
      logger.info(`Report creator ${creator} is NOT in the allowed list for reportId=${reportId}`);
    }
    
    return matches;
  } catch (error) {
    logger.error(`Error checking creator for reportId=${reportId}`, error);
    return false;
  }
}

/************************************************************
 *   MAIN FUNCTION
 ************************************************************/
async function main() {
  logger.info(`Starting Enhanced Dispute Bot: Multi-provider with JSON-RPC batching`);
  
  // Initialize the RPC system
  initializeRpcSystem();
  
  logger.info(`Bot account: ${account.address}`);
  
  // Log configuration
  logger.info(`Bot configuration:`, {
    creatorFilterEnabled: CONFIG.CREATOR_FILTER_ENABLED,
    creatorFilter: CONFIG.CREATOR_FILTER_ENABLED ? 
      (CONFIG.CREATOR_FILTER.length > 0 ? CONFIG.CREATOR_FILTER : 'Empty (rejecting all creators)') : 
      'Disabled (accepting all creators)',
    priceUpdateInterval: `${CONFIG.PRICE_UPDATE_INTERVAL_MS}ms`,
    maxPriceCacheAge: `${CONFIG.MAX_PRICE_CACHE_AGE_MS}ms`,
    logLevel: CONFIG.LOG_LEVEL,
    detailedLogging: CONFIG.DETAILED_LOGGING
  });
  
  // Check account balance at startup
  try {
    const balance = await web3Instances[0].eth.getBalance(account.address);
    const balanceEth = web3Instances[0].utils.fromWei(balance, 'ether');
    logger.info(`Wallet balance: ${balanceEth} ETH`);
    if (Number(balanceEth) < 0.01) {
      logger.warn(`WARNING: Low balance (${balanceEth} ETH). Transactions might fail.`);
    }
  } catch (err) {
    logger.error(`Error checking balance: ${err.message}`);
    
    // If initial connection has rate limiting, apply backoff immediately
    if (isRateLimitError(err)) {
      increaseBackoff();
    }
  }
  
  // Initialize price cache with a warm-up call
  try {
    logger.info(`Initializing price cache...`);
    await updatePriceCache();
    logger.info(`Price cache initialized successfully`);
  } catch (err) {
    logger.warn(`Failed to initialize price cache: ${err.message}`);
  }
  
  logger.info(`Price cache configuration: TTL = ${priceCache.CACHE_TTL_MS}ms, Max Age = ${CONFIG.MAX_PRICE_CACHE_AGE_MS}ms`);
  
  logger.info(`Kraken rate limiting: ${krakenRateLimiter.MIN_INTERVAL_MS}ms between calls`);
  logger.info(`Event poll interval: ${MIN_POLL_INTERVAL_MS}ms (min) to ${MAX_POLL_INTERVAL_MS}ms (max)`);
  logger.info(`Memory cleanup interval: ${MEMORY_CLEANUP_INTERVAL_MS}ms`);
  logger.info(`Retry system: up to ${MAX_RETRIES} retries with exponential backoff (base: ${RETRY_BACKOFF_BASE_MS}ms)`);
  logger.info(`Batch processing system: ${eventQueue.pendingReports.length} events in queue`);
  logger.info(`Profitability threshold: ${MIN_PROFITABILITY_THRESHOLD * 100}%`);
  logger.info(`Multi-provider system: ${RPC_CONFIG.providers.filter(p => p.enabled).length} active providers`);
  
  // Set up regular price updates on a fixed interval
  logger.info(`Setting up regular price updates every ${CONFIG.PRICE_UPDATE_INTERVAL_MS}ms`);
  const priceUpdateInterval = setInterval(updatePriceCache, CONFIG.PRICE_UPDATE_INTERVAL_MS);
  
  // Initial check with a small delay to allow system to initialize
  setTimeout(() => {
    checkForReportsToDispute();
    
    // Set up the regular interval after the first check with dynamic interval
    let pollIntervalId = setInterval(() => {
      checkForReportsToDispute();
    }, currentPollInterval);
    
    // Update poll interval when backoff changes
    setInterval(() => {
      // If poll interval has changed, reset the interval
      if (pollIntervalId && currentPollInterval !== parseInt(pollIntervalId._idleTimeout)) {
        clearInterval(pollIntervalId);
        pollIntervalId = setInterval(() => {
          checkForReportsToDispute();
        }, currentPollInterval);
        
        logger.info(`Updated polling interval to ${currentPollInterval}ms`);
      }
    }, 5000); // Check every 5 seconds if poll interval needs updating
    
    // Set up memory cleanup interval
    setInterval(purgeOldData, MEMORY_CLEANUP_INTERVAL_MS);
    
    // Set up batch processing interval to handle the queue between main cycles
    const BATCH_PROCESSING_INTERVAL_MS = 1000; // 1 second
    setInterval(async () => {
      if (eventQueue.pendingReports.length > 0) {
        const BATCH_SIZE = 5;
        await eventQueue.processBatch(BATCH_SIZE);
      }
    }, BATCH_PROCESSING_INTERVAL_MS);
  }, 2000);
}

main();