const path = require('node:path');
const { randomUUID, pbkdf2Sync } = require('node:crypto');
const { Worker } = require('node:worker_threads');

const express = require('express');
const pinoHttp = require('pino-http');
const pLimit = require('p-limit');
const { Queue, QueueEvents } = require('bullmq');
const client = require('prom-client');

const { createLogger } = require('../../shared/logger');
const { startTracing, shutdownTracing } = require('../../shared/tracing');
const { fetchJsonWithResilience, isTimeoutError } = require('../../shared/http');
const { createRedisClient } = require('../../shared/redis');
const { getQueueDepth } = require('../../shared/queues');
const { QUEUES } = require('../../shared/queue-names');
const {
  dealCardKey,
  parseDealCard,
  buildDealCardSnapshot,
  isDealCardFresh,
  mergeDealWithSnapshot
} = require('../../shared/read-model');
const { sleep, randomInt } = require('../../shared/utils');

const serviceName = 'experience-api';
const sdk = startTracing(serviceName);
const logger = createLogger(serviceName);

const app = express();
const port = Number(process.env.PORT || 3000);
const upstreamTimeoutMs = Number(process.env.UPSTREAM_TIMEOUT_MS || 800);
const enrichmentConcurrency = Number(process.env.ENRICHMENT_CONCURRENCY || 30);
const enrichmentQueueThreshold = Number(process.env.ENRICHMENT_QUEUE_THRESHOLD || 1000);
const hybridReadModelEnabled = process.env.HYBRID_READ_MODEL_ENABLED !== 'false';
const readModelStaleMs = Math.max(Number(process.env.READ_MODEL_STALE_MS || 120_000), 1000);
const readModelTtlSeconds = Math.max(Number(process.env.READ_MODEL_TTL_SECONDS || 300), 30);
const readModelLiveFallbackMax = Math.max(Number(process.env.READ_MODEL_LIVE_FALLBACK_MAX || 2), 0);
const readModelRefreshCooldownMs = Math.max(
  Number(process.env.READ_MODEL_REFRESH_COOLDOWN_MS || 15_000),
  500
);
const baseDealsCacheTtlMs = Math.max(Number(process.env.BASE_DEALS_CACHE_TTL_MS || 3_000), 250);

const upstream = {
  deal: process.env.DEAL_SERVICE_URL || 'http://localhost:3001',
  price: process.env.PRICE_SERVICE_URL || 'http://localhost:3002',
  inventory: process.env.INVENTORY_SERVICE_URL || 'http://localhost:3003',
  rating: process.env.RATING_SERVICE_URL || 'http://localhost:3004',
  merchant: process.env.MERCHANT_SERVICE_URL || 'http://localhost:3005'
};

const redisPrimary = createRedisClient('experience-api-primary');
const redisEnrichmentEvents = createRedisClient('experience-api-enrichment-events');
const redisCriticalEvents = createRedisClient('experience-api-critical-events');

const enrichmentQueue = new Queue(QUEUES.ENRICHMENT, {
  connection: redisPrimary,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 200
    },
    removeOnComplete: 5000,
    removeOnFail: 5000
  }
});

const criticalQueue = new Queue(QUEUES.CRITICAL, {
  connection: redisPrimary,
  defaultJobOptions: {
    attempts: 4,
    backoff: {
      type: 'exponential',
      delay: 250
    },
    removeOnComplete: 5000,
    removeOnFail: 5000
  }
});

const enrichmentQueueEvents = new QueueEvents(QUEUES.ENRICHMENT, {
  connection: redisEnrichmentEvents
});

const criticalQueueEvents = new QueueEvents(QUEUES.CRITICAL, {
  connection: redisCriticalEvents
});

const register = new client.Registry();
client.collectDefaultMetrics({
  register,
  prefix: 'experience_nodejs_'
});

const httpRequestDuration = new client.Histogram({
  name: 'experience_http_request_duration_seconds',
  help: 'HTTP request latency in seconds for experience-api',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.01, 0.03, 0.05, 0.1, 0.2, 0.3, 0.6, 1, 2, 4],
  registers: [register]
});

const httpRequestErrors = new client.Counter({
  name: 'experience_http_request_errors_total',
  help: 'Total HTTP 5xx responses from experience-api',
  labelNames: ['method', 'route', 'status'],
  registers: [register]
});

const upstreamRequestDuration = new client.Histogram({
  name: 'experience_upstream_request_duration_seconds',
  help: 'Upstream request latency in seconds',
  labelNames: ['service', 'status'],
  buckets: [0.02, 0.05, 0.1, 0.2, 0.4, 0.8, 1.2, 2],
  registers: [register]
});

const upstreamRequestsTotal = new client.Counter({
  name: 'experience_upstream_requests_total',
  help: 'Total upstream requests (including retries)',
  labelNames: ['service', 'status'],
  registers: [register]
});

const upstreamFailuresTotal = new client.Counter({
  name: 'experience_upstream_failures_total',
  help: 'Total upstream failures after resilience policy',
  labelNames: ['service', 'reason'],
  registers: [register]
});

const queueDepthGauge = new client.Gauge({
  name: 'experience_queue_depth',
  help: 'Current queue depth by queue',
  labelNames: ['queue'],
  registers: [register]
});

const queueJobLatency = new client.Histogram({
  name: 'experience_queue_job_latency_seconds',
  help: 'Queue end-to-end latency from enqueue to completion/failure',
  labelNames: ['queue', 'status'],
  buckets: [0.05, 0.2, 0.5, 1, 2, 5, 10, 20, 40],
  registers: [register]
});

const queueJobsCompleted = new client.Counter({
  name: 'experience_queue_jobs_completed_total',
  help: 'Completed jobs by queue',
  labelNames: ['queue'],
  registers: [register]
});

const queueJobsFailed = new client.Counter({
  name: 'experience_queue_jobs_failed_total',
  help: 'Failed jobs by queue',
  labelNames: ['queue'],
  registers: [register]
});

const readModelOutcomes = new client.Counter({
  name: 'experience_read_model_outcomes_total',
  help: 'Read-model outcomes for hybrid list aggregation',
  labelNames: ['outcome'],
  registers: [register]
});

const catalogCacheOutcomes = new client.Counter({
  name: 'experience_catalog_cache_outcomes_total',
  help: 'Catalog list cache outcomes in experience-api',
  labelNames: ['outcome'],
  registers: [register]
});

let shuttingDown = false;
let inFlight = 0;
const baseDealsCache = new Map();
const readModelRefreshThrottle = new Map();

const upstreamLimit = pLimit(enrichmentConcurrency);

app.use(express.json({ limit: '2mb' }));

app.use(
  pinoHttp({
    logger,
    genReqId: (req, res) => {
      const requestId = req.headers['x-correlation-id'] || randomUUID();
      res.setHeader('x-correlation-id', requestId);
      return requestId;
    },
    customSuccessMessage: (req, res) => `${req.method} ${req.url} -> ${res.statusCode}`
  })
);

app.use((req, res, next) => {
  if (shuttingDown) {
    res.status(503).json({
      error: 'Service shutting down',
      meta: { requestId: req.id }
    });
    return;
  }

  inFlight += 1;
  const started = process.hrtime.bigint();

  res.on('finish', () => {
    inFlight = Math.max(0, inFlight - 1);

    const route = req.route?.path || req.path || 'unknown';
    const status = String(res.statusCode);
    const durationSeconds = Number(process.hrtime.bigint() - started) / 1e9;

    httpRequestDuration.observe(
      {
        method: req.method,
        route,
        status
      },
      durationSeconds
    );

    if (res.statusCode >= 500) {
      httpRequestErrors.inc({
        method: req.method,
        route,
        status
      });
    }
  });

  next();
});

function createUpstreamMetricsHandler(service) {
  return {
    onAttempt: ({ durationSeconds, status }) => {
      upstreamRequestDuration.observe({ service, status }, durationSeconds);
      upstreamRequestsTotal.inc({ service, status });
    }
  };
}

async function getUpstreamJson({
  service,
  url,
  requestId,
  retries = 1,
  method = 'GET',
  body,
  warningSet,
  allowDegrade = true
}) {
  try {
    const result = await upstreamLimit(() =>
      fetchJsonWithResilience({
        url,
        method,
        body,
        timeoutMs: upstreamTimeoutMs,
        retries,
        requestId,
        serviceName: service,
        logger,
        metrics: createUpstreamMetricsHandler(service)
      })
    );

    return result;
  } catch (error) {
    const timeout = isTimeoutError(error);

    upstreamFailuresTotal.inc({
      service,
      reason: timeout ? 'timeout' : 'error'
    });

    if (allowDegrade && warningSet) {
      warningSet.add(timeout ? `${service} timeout` : `${service} unavailable`);
      return null;
    }

    throw error;
  }
}

async function enrichDeal(deal, requestId, warningSet) {
  const [pricingPayload, inventoryPayload, ratingPayload, merchantPayload] = await Promise.all([
    getUpstreamJson({
      service: 'price-service',
      url: `${upstream.price}/price/${deal.id}`,
      requestId,
      warningSet,
      retries: 1,
      allowDegrade: true
    }),
    getUpstreamJson({
      service: 'inventory-service',
      url: `${upstream.inventory}/inventory/${deal.id}`,
      requestId,
      warningSet,
      retries: 1,
      allowDegrade: true
    }),
    getUpstreamJson({
      service: 'rating-lb',
      url: `${upstream.rating}/rating/${deal.id}`,
      requestId,
      warningSet,
      retries: 1,
      allowDegrade: true
    }),
    getUpstreamJson({
      service: 'merchant-service',
      url: `${upstream.merchant}/merchant/${deal.merchantId}`,
      requestId,
      warningSet,
      retries: 1,
      allowDegrade: true
    })
  ]);

  return {
    ...deal,
    pricing: pricingPayload?.pricing || null,
    inventory: inventoryPayload?.inventory || null,
    rating: ratingPayload?.rating || null,
    merchant: merchantPayload?.merchant || null
  };
}

function catalogCacheKey(city, limit) {
  return `${city || '*'}:${limit}`;
}

function degradedDeal(deal) {
  return {
    ...deal,
    pricing: null,
    inventory: null,
    rating: null,
    merchant: null
  };
}

async function fetchBaseDeals(city, limit, requestId) {
  return getUpstreamJson({
    service: 'deal-service',
    url: `${upstream.deal}/deals?city=${encodeURIComponent(city)}&limit=${limit}`,
    requestId,
    retries: 1,
    allowDegrade: false
  });
}

async function getBaseDealsWithCache(city, limit, requestId) {
  const key = catalogCacheKey(city, limit);
  const now = Date.now();
  const cached = baseDealsCache.get(key);

  if (cached && cached.expiresAt > now) {
    catalogCacheOutcomes.inc({ outcome: 'hit' });
    return cached.payload;
  }

  catalogCacheOutcomes.inc({ outcome: 'miss' });
  const payload = await fetchBaseDeals(city, limit, requestId);
  baseDealsCache.set(key, {
    payload,
    expiresAt: now + baseDealsCacheTtlMs
  });
  return payload;
}

async function fetchReadModelSnapshots(baseDeals) {
  if (!baseDeals.length) {
    return new Map();
  }

  const keys = baseDeals.map((deal) => dealCardKey(deal.id));
  const rows = await redisPrimary.mget(keys);
  const snapshots = new Map();

  for (let index = 0; index < baseDeals.length; index += 1) {
    const parsed = parseDealCard(rows[index]);
    if (parsed) {
      snapshots.set(baseDeals[index].id, parsed);
    }
  }

  return snapshots;
}

function shouldScheduleReadModelRefresh(dealId) {
  const now = Date.now();
  const nextAllowed = readModelRefreshThrottle.get(dealId) || 0;

  if (nextAllowed > now) {
    return false;
  }

  readModelRefreshThrottle.set(dealId, now + readModelRefreshCooldownMs);
  return true;
}

async function enqueueReadModelRefreshes(deals, requestId) {
  if (!deals.length) {
    return 0;
  }

  const enqueuedAt = Date.now();
  const jobs = [];

  for (const deal of deals) {
    if (!deal?.id || !shouldScheduleReadModelRefresh(deal.id)) {
      continue;
    }

    jobs.push({
      name: 'enrich-deal',
      data: {
        requestId,
        city: deal.city,
        merchantId: deal.merchantId || null,
        dealId: deal.id,
        enqueuedAt
      },
      opts: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 200
        },
        removeOnComplete: 5000,
        removeOnFail: 5000
      }
    });
  }

  if (!jobs.length) {
    return 0;
  }

  await enrichmentQueue.addBulk(jobs);
  return jobs.length;
}

async function writeReadModelSnapshotFromDeal(deal, source, warnings = []) {
  if (!deal?.id) {
    return;
  }

  const snapshot = buildDealCardSnapshot({
    dealId: deal.id,
    city: deal.city || null,
    merchantId: deal.merchantId || null,
    pricing: deal.pricing || null,
    inventory: deal.inventory || null,
    rating: deal.rating || null,
    merchant: deal.merchant || null,
    source,
    warnings
  });

  await redisPrimary.set(dealCardKey(deal.id), JSON.stringify(snapshot), 'EX', readModelTtlSeconds);
}

function runHashInWorker(payload) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, 'hash-worker.js'));

    worker.once('message', (message) => {
      worker.terminate();

      if (message.error) {
        reject(new Error(message.error));
        return;
      }

      resolve(message);
    });

    worker.once('error', (error) => {
      worker.terminate();
      reject(error);
    });

    worker.postMessage(payload);
  });
}

function runBlockingHash({ payload, rounds, iterations }) {
  let digest = '';

  for (let i = 0; i < rounds; i += 1) {
    digest = pbkdf2Sync(payload, `salt-${i}`, iterations, 64, 'sha512').toString('hex');
  }

  return digest;
}

async function refreshQueueDepthMetrics() {
  const [enrichmentDepth, criticalDepth] = await Promise.all([
    getQueueDepth(enrichmentQueue),
    getQueueDepth(criticalQueue)
  ]);

  queueDepthGauge.set({ queue: QUEUES.ENRICHMENT }, enrichmentDepth);
  queueDepthGauge.set({ queue: QUEUES.CRITICAL }, criticalDepth);
}

async function observeQueueJobLatency(queue, queueName, status, jobId) {
  const job = await queue.getJob(jobId);

  if (!job || !job.finishedOn) {
    return;
  }

  const latencySeconds = Math.max(0, (job.finishedOn - job.timestamp) / 1000);
  queueJobLatency.observe({ queue: queueName, status }, latencySeconds);

  if (status === 'completed') {
    queueJobsCompleted.inc({ queue: queueName });
  } else {
    queueJobsFailed.inc({ queue: queueName });
  }
}

enrichmentQueueEvents.on('completed', ({ jobId }) => {
  observeQueueJobLatency(enrichmentQueue, QUEUES.ENRICHMENT, 'completed', jobId).catch((error) => {
    logger.warn({ err: error, jobId }, 'Failed to observe enrichment completed metrics');
  });
});

enrichmentQueueEvents.on('failed', ({ jobId }) => {
  observeQueueJobLatency(enrichmentQueue, QUEUES.ENRICHMENT, 'failed', jobId).catch((error) => {
    logger.warn({ err: error, jobId }, 'Failed to observe enrichment failed metrics');
  });
});

criticalQueueEvents.on('completed', ({ jobId }) => {
  observeQueueJobLatency(criticalQueue, QUEUES.CRITICAL, 'completed', jobId).catch((error) => {
    logger.warn({ err: error, jobId }, 'Failed to observe critical completed metrics');
  });
});

criticalQueueEvents.on('failed', ({ jobId }) => {
  observeQueueJobLatency(criticalQueue, QUEUES.CRITICAL, 'failed', jobId).catch((error) => {
    logger.warn({ err: error, jobId }, 'Failed to observe critical failed metrics');
  });
});

const queueMetricInterval = setInterval(() => {
  refreshQueueDepthMetrics().catch((error) => {
    logger.warn({ err: error }, 'Failed to refresh queue depth metrics');
  });
}, 2000);

queueMetricInterval.unref();

app.get('/api/deals', async (req, res) => {
  const city = (req.query.city || 'valencia').toString().toLowerCase().trim();
  const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 100);
  const warningSet = new Set();

  let baseDealsPayload;
  try {
    baseDealsPayload = await getBaseDealsWithCache(city, limit, req.id);
  } catch (error) {
    req.log.error({ err: error }, 'Failed to fetch base deals');
    res.status(502).json({
      error: 'deal-service unavailable',
      warnings: ['catalog unavailable'],
      meta: { requestId: req.id }
    });
    return;
  }

  const baseDeals = baseDealsPayload.deals || [];

  if (!hybridReadModelEnabled) {
    // Dangerous anti-pattern for large fan-out traffic:
    //   await Promise.all(10000Calls)
    // This can saturate memory, sockets, and upstream capacity at once.
    // Safer pattern: each upstream call is wrapped in p-limit so we cap in-flight fan-out.
    const enrichedDeals = await Promise.all(baseDeals.map((deal) => enrichDeal(deal, req.id, warningSet)));

    res.json({
      deals: enrichedDeals,
      warnings: [...warningSet],
      meta: {
        requestId: req.id,
        mode: 'live-composition'
      }
    });
    return;
  }

  let snapshots = new Map();
  try {
    snapshots = await fetchReadModelSnapshots(baseDeals);
  } catch (error) {
    req.log.warn({ err: error }, 'Failed to read deal-card read model from Redis');
    warningSet.add('read-model unavailable, using live fallback');
  }

  const nowMs = Date.now();
  const resolvedByDealId = new Map();
  const missingDeals = [];
  const staleDeals = [];
  const refreshCandidates = [];

  for (const deal of baseDeals) {
    const snapshot = snapshots.get(deal.id);

    if (!snapshot) {
      missingDeals.push(deal);
      refreshCandidates.push(deal);
      continue;
    }

    if (isDealCardFresh(snapshot, readModelStaleMs, nowMs)) {
      resolvedByDealId.set(deal.id, mergeDealWithSnapshot(deal, snapshot));
      continue;
    }

    staleDeals.push(deal);
    refreshCandidates.push(deal);
    resolvedByDealId.set(deal.id, mergeDealWithSnapshot(deal, snapshot));
  }

  const hits = baseDeals.length - missingDeals.length - staleDeals.length;
  if (hits > 0) {
    readModelOutcomes.inc({ outcome: 'hit' }, hits);
  }
  if (missingDeals.length > 0) {
    readModelOutcomes.inc({ outcome: 'miss' }, missingDeals.length);
  }
  if (staleDeals.length > 0) {
    readModelOutcomes.inc({ outcome: 'stale' }, staleDeals.length);
  }

  const fallbackCandidates = [...missingDeals, ...staleDeals];
  const fallbackDeals = fallbackCandidates.slice(0, readModelLiveFallbackMax);
  let fallbackCount = 0;

  for (const deal of fallbackDeals) {
    const enriched = await enrichDeal(deal, req.id, warningSet);
    resolvedByDealId.set(deal.id, enriched);
    fallbackCount += 1;

    writeReadModelSnapshotFromDeal(enriched, 'experience-api-live-fallback').catch((error) => {
      req.log.warn({ err: error, dealId: deal.id }, 'Failed to write read-model snapshot from live fallback');
    });
  }

  if (fallbackCount > 0) {
    readModelOutcomes.inc({ outcome: 'live_fallback' }, fallbackCount);
  }

  let refreshEnqueued = 0;
  try {
    refreshEnqueued = await enqueueReadModelRefreshes(refreshCandidates, req.id);
  } catch (error) {
    req.log.warn({ err: error }, 'Failed to enqueue read-model refresh jobs');
    warningSet.add('read-model refresh enqueue failed');
  }

  if (refreshEnqueued > 0) {
    readModelOutcomes.inc({ outcome: 'refresh_enqueued' }, refreshEnqueued);
  }

  const hybridDeals = baseDeals.map((deal) => resolvedByDealId.get(deal.id) || degradedDeal(deal));

  if (missingDeals.length > 0) {
    warningSet.add(`hybrid read-model misses: ${missingDeals.length}`);
  }
  if (staleDeals.length > 0) {
    warningSet.add(`hybrid read-model stale served: ${staleDeals.length}`);
  }
  if (refreshEnqueued > 0) {
    warningSet.add(`hybrid read-model refresh queued: ${refreshEnqueued}`);
  }

  // Dangerous anti-pattern for large fan-out traffic:
  //   await Promise.all(10000Calls)
  // This can saturate memory, sockets, and upstream capacity at once.
  // Hybrid pattern used here: serve from read model and only live-enrich a small fallback set.
  res.json({
    deals: hybridDeals,
    warnings: [...warningSet],
    meta: {
      requestId: req.id,
      mode: 'hybrid-read-model',
      readModel: {
        hits,
        stale: staleDeals.length,
        misses: missingDeals.length,
        liveFallback: fallbackCount,
        refreshEnqueued
      }
    }
  });
});

app.get('/api/deals/:id', async (req, res) => {
  const warningSet = new Set();

  let dealPayload;
  try {
    dealPayload = await getUpstreamJson({
      service: 'deal-service',
      url: `${upstream.deal}/deals/${req.params.id}`,
      requestId: req.id,
      retries: 1,
      allowDegrade: false
    });
  } catch (error) {
    if (error.statusCode === 404) {
      res.status(404).json({
        error: 'Deal not found',
        meta: { requestId: req.id }
      });
      return;
    }

    res.status(502).json({
      error: 'deal-service unavailable',
      meta: { requestId: req.id }
    });
    return;
  }

  const enrichedDeal = await enrichDeal(dealPayload.deal, req.id, warningSet);

  writeReadModelSnapshotFromDeal(enrichedDeal, 'experience-api-detail').catch((error) => {
    req.log.warn({ err: error, dealId: enrichedDeal.id }, 'Failed to write read-model snapshot from detail');
  });

  res.json({
    deal: enrichedDeal,
    warnings: [...warningSet],
    meta: { requestId: req.id }
  });
});

app.post('/api/purchase', async (req, res) => {
  const dealId = req.body?.dealId;
  const userId = req.body?.userId || `u-${randomInt(100, 999)}`;
  const quantity = Math.min(Math.max(Number(req.body?.quantity) || 1, 1), 10);
  const warningSet = new Set();

  if (!dealId) {
    res.status(400).json({
      error: 'dealId is required',
      meta: { requestId: req.id }
    });
    return;
  }

  const inventoryPayload = await getUpstreamJson({
    service: 'inventory-service',
    url: `${upstream.inventory}/inventory/${dealId}`,
    requestId: req.id,
    retries: 1,
    warningSet,
    allowDegrade: true
  });

  if (inventoryPayload?.inventory && !inventoryPayload.inventory.available) {
    res.status(409).json({
      error: 'Deal is out of stock',
      warnings: [...warningSet],
      meta: { requestId: req.id }
    });
    return;
  }

  const job = await criticalQueue.add(
    'purchase-workflow',
    {
      dealId,
      userId,
      quantity,
      requestId: req.id,
      createdAt: Date.now()
    },
    {
      attempts: 4,
      backoff: {
        type: 'exponential',
        delay: 250
      }
    }
  );

  res.status(202).json({
    accepted: true,
    jobId: job.id,
    warnings: [...warningSet],
    meta: { requestId: req.id }
  });
});

app.post('/api/enrichment-jobs', async (req, res) => {
  const requestedCount = Number(req.body?.count) || 1000;
  const count = Math.min(Math.max(requestedCount, 1), 10_000);
  const city = (req.body?.city || 'valencia').toString().toLowerCase().trim();
  const depth = await getQueueDepth(enrichmentQueue);

  queueDepthGauge.set({ queue: QUEUES.ENRICHMENT }, depth);

  if (depth >= enrichmentQueueThreshold) {
    res.status(429).json({
      error: 'Queue overloaded, backpressure admission control triggered',
      queueDepth: depth,
      threshold: enrichmentQueueThreshold,
      meta: { requestId: req.id }
    });
    return;
  }

  const now = Date.now();

  const jobs = Array.from({ length: count }).map((_, index) => {
    const numericDealId = ((index % 220) + 1).toString().padStart(5, '0');
    const merchantNumericId = ((index % 10) + 1).toString().padStart(3, '0');
    return {
      name: 'enrich-deal',
      data: {
        requestId: req.id,
        city,
        dealId: `d-${numericDealId}`,
        merchantId: `m-${merchantNumericId}`,
        enqueuedAt: now
      },
      opts: {
        attempts: 5,
        backoff: {
          type: 'exponential',
          delay: 200
        },
        removeOnComplete: 5000,
        removeOnFail: 5000
      }
    };
  });

  await enrichmentQueue.addBulk(jobs);

  const updatedDepth = await getQueueDepth(enrichmentQueue);
  queueDepthGauge.set({ queue: QUEUES.ENRICHMENT }, updatedDepth);

  res.status(202).json({
    accepted: true,
    enqueued: count,
    queueDepth: updatedDepth,
    meta: { requestId: req.id }
  });
});

app.post('/api/heavy-hash/bad', async (req, res) => {
  const payload = req.body?.payload || `deal-user-${req.id}`;
  const rounds = Math.min(Math.max(Number(req.body?.rounds) || 3, 1), 8);
  const iterations = Math.min(Math.max(Number(req.body?.iterations) || 220_000, 20_000), 500_000);

  // This intentionally blocks the event loop. Under load, request latency spikes,
  // timeouts cascade across services, and queueing delay increases system-wide.
  const startedAt = Date.now();
  const digest = runBlockingHash({ payload, rounds, iterations });

  res.json({
    mode: 'bad-main-thread',
    durationMs: Date.now() - startedAt,
    digest: digest.slice(0, 24),
    meta: { requestId: req.id }
  });
});

app.post('/api/heavy-hash/good', async (req, res) => {
  const payload = req.body?.payload || `deal-user-${req.id}`;
  const rounds = Math.min(Math.max(Number(req.body?.rounds) || 3, 1), 8);
  const iterations = Math.min(Math.max(Number(req.body?.iterations) || 220_000, 20_000), 500_000);

  // Worker threads move CPU-heavy work off the event loop so API I/O stays responsive.
  const startedAt = Date.now();
  const result = await runHashInWorker({ payload, rounds, iterations });

  res.json({
    mode: 'good-worker-thread',
    durationMs: Date.now() - startedAt,
    workerDurationMs: result.durationMs,
    digest: result.digest.slice(0, 24),
    meta: { requestId: req.id }
  });
});

app.get('/healthz', (req, res) => {
  res.json({
    status: 'ok',
    service: serviceName,
    meta: { requestId: req.id }
  });
});

app.get('/readyz', async (req, res) => {
  const checks = [];

  try {
    const pong = await redisPrimary.ping();
    checks.push({
      dependency: 'redis',
      ok: pong === 'PONG'
    });
  } catch (error) {
    checks.push({
      dependency: 'redis',
      ok: false,
      reason: error.message
    });
  }

  const upstreamChecks = [
    ['deal-service', `${upstream.deal}/healthz`],
    ['price-service', `${upstream.price}/healthz`],
    ['inventory-service', `${upstream.inventory}/healthz`],
    ['rating-lb', `${upstream.rating}/healthz`],
    ['merchant-service', `${upstream.merchant}/healthz`]
  ];

  await Promise.all(
    upstreamChecks.map(async ([service, url]) => {
      try {
        await fetchJsonWithResilience({
          url,
          method: 'GET',
          retries: 0,
          timeoutMs: 500,
          requestId: req.id,
          serviceName: service,
          logger,
          metrics: createUpstreamMetricsHandler(service)
        });

        checks.push({
          dependency: service,
          ok: true
        });
      } catch (error) {
        checks.push({
          dependency: service,
          ok: false,
          reason: isTimeoutError(error) ? 'timeout' : error.message
        });
      }
    })
  );

  const ready = checks.every((check) => check.ok);

  res.status(ready ? 200 : 503).json({
    status: ready ? 'ready' : 'not-ready',
    checks,
    meta: { requestId: req.id }
  });
});

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.send(await register.metrics());
});

app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    meta: { requestId: req.id }
  });
});

const server = app.listen(port, () => {
  logger.info(
    {
      port,
      enrichmentConcurrency,
      enrichmentQueueThreshold,
      hybridReadModelEnabled,
      readModelStaleMs,
      readModelLiveFallbackMax,
      baseDealsCacheTtlMs
    },
    'experience-api listening'
  );
});

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal }, 'Starting graceful shutdown');

  clearInterval(queueMetricInterval);

  await new Promise((resolve) => {
    server.close(() => resolve());
  });

  const deadline = Date.now() + 10_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(50);
  }

  await Promise.allSettled([
    enrichmentQueueEvents.close(),
    criticalQueueEvents.close(),
    enrichmentQueue.close(),
    criticalQueue.close(),
    redisPrimary.quit(),
    redisEnrichmentEvents.quit(),
    redisCriticalEvents.quit(),
    shutdownTracing(sdk, logger)
  ]);

  logger.info({ inFlight }, 'experience-api shutdown complete');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM');
});
