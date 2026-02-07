const { randomUUID } = require('node:crypto');

const express = require('express');
const pinoHttp = require('pino-http');
const { fetch } = require('undici');

const { createLogger } = require('../../shared/logger');
const { startTracing, shutdownTracing } = require('../../shared/tracing');
const { sleep } = require('../../shared/utils');

const serviceName = 'rating-lb';
const sdk = startTracing(serviceName);
const logger = createLogger(serviceName);

const app = express();
const port = Number(process.env.PORT || 3014);
const targetUrls = (process.env.LB_TARGETS || 'http://localhost:3004')
  .split(',')
  .map((entry) => entry.trim())
  .filter(Boolean);

const targets = targetUrls.map((url, index) => ({
  id: `rating-service-${String.fromCharCode(97 + index)}`,
  url,
  totalRequests: 0,
  totalErrors: 0,
  healthy: null,
  lastStatusCode: null,
  lastError: null,
  lastUsedAt: null
}));

let cursor = 0;
let shuttingDown = false;
let inFlight = 0;

app.use(express.json({ limit: '2mb' }));

app.use(
  pinoHttp({
    logger,
    genReqId: (req, res) => {
      const requestId = req.headers['x-correlation-id'] || randomUUID();
      res.setHeader('x-correlation-id', requestId);
      return requestId;
    }
  })
);

app.use((req, res, next) => {
  if (shuttingDown) {
    res.status(503).json({
      error: 'rating-lb shutting down',
      requestId: req.id
    });
    return;
  }

  inFlight += 1;
  res.on('finish', () => {
    inFlight = Math.max(0, inFlight - 1);
  });

  next();
});

function choosePrimaryTarget() {
  if (targets.length === 0) {
    return null;
  }

  const target = targets[cursor % targets.length];
  cursor = (cursor + 1) % targets.length;
  return target;
}

function chooseFallbackTarget(primaryTarget) {
  if (targets.length < 2 || !primaryTarget) {
    return null;
  }

  const primaryIndex = targets.findIndex((target) => target.id === primaryTarget.id);
  const fallbackIndex = primaryIndex >= 0 ? (primaryIndex + 1) % targets.length : 0;

  return targets[fallbackIndex];
}

async function forwardRatingRequest(target, req) {
  const response = await fetch(`${target.url}/rating/${encodeURIComponent(req.params.dealId)}`, {
    method: 'GET',
    headers: {
      'content-type': 'application/json',
      'x-correlation-id': req.id
    },
    signal: AbortSignal.timeout(900)
  });

  const bodyText = await response.text();

  target.totalRequests += 1;
  target.lastUsedAt = new Date().toISOString();
  target.lastStatusCode = response.status;

  if (!response.ok) {
    target.totalErrors += 1;
    target.lastError = `HTTP ${response.status}`;
    throw new Error(`Target ${target.id} failed with status ${response.status}: ${bodyText.slice(0, 180)}`);
  }

  target.lastError = null;
  return {
    statusCode: response.status,
    bodyText,
    contentType: response.headers.get('content-type') || 'application/json'
  };
}

app.get('/rating/:dealId', async (req, res) => {
  const primaryTarget = choosePrimaryTarget();

  if (!primaryTarget) {
    res.status(503).json({
      error: 'No rating targets configured',
      requestId: req.id
    });
    return;
  }

  const fallbackTarget = chooseFallbackTarget(primaryTarget);

  try {
    const result = await forwardRatingRequest(primaryTarget, req);

    res.set('content-type', result.contentType);
    res.set('x-lb-target', primaryTarget.id);
    res.status(result.statusCode).send(result.bodyText);
    return;
  } catch (firstError) {
    req.log.warn(
      {
        err: firstError,
        target: primaryTarget.id
      },
      'Primary rating target failed'
    );

    if (!fallbackTarget) {
      res.status(503).json({
        error: 'rating-lb all targets failed',
        requestId: req.id
      });
      return;
    }

    try {
      const result = await forwardRatingRequest(fallbackTarget, req);

      res.set('content-type', result.contentType);
      res.set('x-lb-target', fallbackTarget.id);
      res.status(result.statusCode).send(result.bodyText);
      return;
    } catch (secondError) {
      req.log.error(
        {
          err: secondError,
          primaryTarget: primaryTarget.id,
          fallbackTarget: fallbackTarget.id
        },
        'All rating targets failed'
      );

      res.status(503).json({
        error: 'rating-lb all targets failed',
        requestId: req.id
      });
    }
  }
});

app.get('/stats', (_req, res) => {
  const totalRequests = targets.reduce((acc, target) => acc + target.totalRequests, 0);
  const totalErrors = targets.reduce((acc, target) => acc + target.totalErrors, 0);

  res.json({
    timestamp: new Date().toISOString(),
    service: serviceName,
    strategy: 'round-robin',
    totalRequests,
    totalErrors,
    targets: targets.map((target) => ({
      id: target.id,
      url: target.url,
      totalRequests: target.totalRequests,
      totalErrors: target.totalErrors,
      healthy: target.healthy,
      lastStatusCode: target.lastStatusCode,
      lastError: target.lastError,
      lastUsedAt: target.lastUsedAt
    }))
  });
});

app.get('/healthz', async (req, res) => {
  const checks = await Promise.all(
    targets.map(async (target) => {
      try {
        const response = await fetch(`${target.url}/healthz`, {
          headers: {
            'x-correlation-id': req.id
          },
          signal: AbortSignal.timeout(700)
        });

        target.healthy = response.ok;
        if (!response.ok) {
          target.lastError = `healthz status ${response.status}`;
        }

        return {
          target: target.id,
          ok: response.ok,
          statusCode: response.status
        };
      } catch (error) {
        target.healthy = false;
        target.lastError = error.message;

        return {
          target: target.id,
          ok: false,
          reason: error.message
        };
      }
    })
  );

  const healthy = checks.every((check) => check.ok);

  res.status(healthy ? 200 : 503).json({
    service: serviceName,
    status: healthy ? 'ok' : 'degraded',
    checks,
    requestId: req.id
  });
});

const server = app.listen(port, () => {
  logger.info({ port, targets: targets.map((target) => target.url) }, 'rating-lb listening');
});

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal }, 'Starting rating-lb shutdown');

  await new Promise((resolve) => {
    server.close(() => resolve());
  });

  const deadline = Date.now() + 10_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(40);
  }

  await shutdownTracing(sdk, logger);
  logger.info({ inFlight }, 'rating-lb stopped');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM');
});
