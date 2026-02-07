const { Worker, Queue } = require('bullmq');

const { createLogger } = require('../shared/logger');
const { startTracing, shutdownTracing } = require('../shared/tracing');
const { createRedisClient } = require('../shared/redis');
const { QUEUES } = require('../shared/queue-names');
const { chance, randomInt, sleep } = require('../shared/utils');

const serviceName = 'critical-worker';
const sdk = startTracing(serviceName);
const logger = createLogger(serviceName);

const concurrency = Number(process.env.CRITICAL_WORKER_CONCURRENCY || 25);

const redis = createRedisClient('critical-worker');
const dlqRedis = createRedisClient('critical-dlq-writer');

const criticalDlqQueue = new Queue(QUEUES.CRITICAL_DLQ, {
  connection: dlqRedis,
  defaultJobOptions: {
    removeOnComplete: 10_000,
    removeOnFail: 10_000
  }
});

async function fraudCheck(job) {
  await sleep(randomInt(20, 140));

  if (chance(0.08)) {
    throw new Error('Fraud-check provider timeout');
  }

  return {
    riskScore: Number((Math.random() * 100).toFixed(2)),
    approved: true
  };
}

async function emitAnalytics(job) {
  await sleep(randomInt(10, 70));

  if (chance(0.03)) {
    throw new Error('Analytics pipeline backpressure');
  }

  return {
    event: 'purchase_completed',
    userId: job.data.userId,
    dealId: job.data.dealId
  };
}

async function sendReceipt(job) {
  await sleep(randomInt(30, 110));

  if (chance(0.05)) {
    throw new Error('Email API transient failure');
  }

  return {
    sent: true,
    to: `${job.data.userId}@example.local`
  };
}

const worker = new Worker(
  QUEUES.CRITICAL,
  async (job) => {
    const startedAt = Date.now();
    const requestId = job.data.requestId || `critical-job-${job.id}`;

    const fraudResult = await fraudCheck(job);
    const analyticsResult = await emitAnalytics(job);
    const emailResult = await sendReceipt(job);

    return {
      requestId,
      dealId: job.data.dealId,
      quantity: job.data.quantity,
      fraudResult,
      analyticsResult,
      emailResult,
      latencyMs: Date.now() - startedAt,
      enqueuedForMs: Date.now() - (job.data.createdAt || startedAt)
    };
  },
  {
    concurrency,
    connection: redis
  }
);

worker.on('ready', () => {
  logger.info({ concurrency }, 'critical-worker ready');
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
    'Critical purchase workflow completed'
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
    'Critical purchase workflow failed'
  );

  const maxAttempts = job?.opts?.attempts || 1;
  if (!job || job.attemptsMade < maxAttempts) {
    return;
  }

  await criticalDlqQueue.add('critical-dlq', {
    originalQueue: QUEUES.CRITICAL,
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
    'Critical job moved to DLQ'
  );
});

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal }, 'Starting critical-worker shutdown');

  await Promise.allSettled([
    worker.close(),
    criticalDlqQueue.close(),
    redis.quit(),
    dlqRedis.quit(),
    shutdownTracing(sdk, logger)
  ]);

  logger.info('critical-worker stopped');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM');
});
