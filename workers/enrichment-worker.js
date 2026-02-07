const { Worker, Queue } = require('bullmq');

const { createLogger } = require('../shared/logger');
const { startTracing, shutdownTracing } = require('../shared/tracing');
const { fetchJsonWithResilience } = require('../shared/http');
const { createRedisClient } = require('../shared/redis');
const { QUEUES } = require('../shared/queue-names');
const { dealCardKey, buildDealCardSnapshot } = require('../shared/read-model');
const { chance, sleep, randomInt } = require('../shared/utils');

const serviceName = 'enrichment-worker';
const sdk = startTracing(serviceName);
const logger = createLogger(serviceName);

const concurrency = Number(process.env.ENRICHMENT_WORKER_CONCURRENCY || 60);
const upstreamTimeoutMs = Number(process.env.WORKER_UPSTREAM_TIMEOUT_MS || 900);
const readModelTtlSeconds = Math.max(Number(process.env.READ_MODEL_TTL_SECONDS || 300), 30);

const upstream = {
  price: process.env.PRICE_SERVICE_URL || 'http://localhost:3002',
  inventory: process.env.INVENTORY_SERVICE_URL || 'http://localhost:3003',
  rating: process.env.RATING_SERVICE_URL || 'http://localhost:3004',
  merchant: process.env.MERCHANT_SERVICE_URL || 'http://localhost:3005'
};

const redis = createRedisClient('enrichment-worker');
const dlqRedis = createRedisClient('enrichment-dlq-writer');

const enrichmentDlqQueue = new Queue(QUEUES.ENRICHMENT_DLQ, {
  connection: dlqRedis,
  defaultJobOptions: {
    removeOnComplete: 10_000,
    removeOnFail: 10_000
  }
});

async function fetchUpstream(service, url, requestId) {
  return fetchJsonWithResilience({
    url,
    retries: 1,
    timeoutMs: upstreamTimeoutMs,
    requestId,
    serviceName: service,
    logger
  });
}

const worker = new Worker(
  QUEUES.ENRICHMENT,
  async (job) => {
    const requestId = job.data.requestId || `enrichment-job-${job.id}`;
    const startedAt = Date.now();

    await sleep(randomInt(10, 45));

    if (chance(0.04)) {
      throw new Error('Synthetic enrichment worker fault');
    }

    const [pricing, inventory, rating, merchant] = await Promise.allSettled([
      fetchUpstream('price-service', `${upstream.price}/price/${job.data.dealId}`, requestId),
      fetchUpstream('inventory-service', `${upstream.inventory}/inventory/${job.data.dealId}`, requestId),
      fetchUpstream('rating-lb', `${upstream.rating}/rating/${job.data.dealId}`, requestId),
      fetchUpstream(
        'merchant-service',
        `${upstream.merchant}/merchant/${job.data.merchantId || 'm-001'}`,
        requestId
      )
    ]);

    const parts = [pricing, inventory, rating, merchant];
    const successCount = parts.filter((part) => part.status === 'fulfilled').length;

    if (successCount < 2) {
      throw new Error('Not enough enrichment dependencies succeeded');
    }

    const snapshot = buildDealCardSnapshot({
      dealId: job.data.dealId,
      city: job.data.city || null,
      merchantId: job.data.merchantId || merchant.value?.merchant?.id || null,
      pricing: pricing.status === 'fulfilled' ? pricing.value?.pricing || null : null,
      inventory: inventory.status === 'fulfilled' ? inventory.value?.inventory || null : null,
      rating: rating.status === 'fulfilled' ? rating.value?.rating || null : null,
      merchant: merchant.status === 'fulfilled' ? merchant.value?.merchant || null : null,
      source: 'enrichment-worker',
      warnings: parts
        .filter((part) => part.status === 'rejected')
        .map((part) => part.reason?.message)
        .filter(Boolean)
    });

    await redis.set(dealCardKey(job.data.dealId), JSON.stringify(snapshot), 'EX', readModelTtlSeconds);

    return {
      requestId,
      dealId: job.data.dealId,
      city: job.data.city,
      successCount,
      readModelUpdated: true,
      latencyMs: Date.now() - startedAt,
      enqueuedForMs: Date.now() - (job.data.enqueuedAt || startedAt)
    };
  },
  {
    concurrency,
    connection: redis
  }
);

worker.on('ready', () => {
  logger.info({ concurrency }, 'enrichment-worker ready');
});

worker.on('completed', (job, result) => {
  logger.info(
    {
      jobId: job.id,
      requestId: result.requestId,
      dealId: result.dealId,
      latencyMs: result.latencyMs,
      enqueuedForMs: result.enqueuedForMs
    },
    'Enrichment job completed'
  );
});

worker.on('failed', async (job, error) => {
  logger.warn(
    {
      jobId: job?.id,
      attemptsMade: job?.attemptsMade,
      maxAttempts: job?.opts?.attempts,
      err: error
    },
    'Enrichment job failed'
  );

  const maxAttempts = job?.opts?.attempts || 1;
  if (!job || job.attemptsMade < maxAttempts) {
    return;
  }

  await enrichmentDlqQueue.add('enrichment-dlq', {
    originalQueue: QUEUES.ENRICHMENT,
    originalJobId: job.id,
    requestId: job.data.requestId,
    payload: job.data,
    failedAt: Date.now(),
    attemptsMade: job.attemptsMade,
    failedReason: error.message
  });

  logger.error(
    {
      originalJobId: job.id,
      requestId: job.data.requestId
    },
    'Enrichment job moved to DLQ'
  );
});

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal }, 'Starting enrichment-worker shutdown');

  await Promise.allSettled([
    worker.close(),
    enrichmentDlqQueue.close(),
    redis.quit(),
    dlqRedis.quit(),
    shutdownTracing(sdk, logger)
  ]);

  logger.info('enrichment-worker stopped');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM');
});
