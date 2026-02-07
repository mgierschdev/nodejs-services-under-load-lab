const express = require('express');
const pinoHttp = require('pino-http');
const { randomUUID } = require('node:crypto');
const { createLogger } = require('./logger');
const { startTracing, shutdownTracing } = require('./tracing');
const { sleep, randomInt, chance } = require('./utils');

function createDomainService({
  serviceName,
  defaultPort,
  latency = { min: 25, max: 220 },
  failureRate = 0.07,
  registerRoutes
}) {
  const sdk = startTracing(serviceName);
  const logger = createLogger(serviceName);
  const app = express();

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
      },
      customSuccessMessage: (req, res) => `${req.method} ${req.url} -> ${res.statusCode}`
    })
  );

  app.use((req, res, next) => {
    if (shuttingDown) {
      res.status(503).json({
        error: 'Service shutting down',
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

  app.get('/healthz', (req, res) => {
    res.json({
      status: 'ok',
      service: serviceName,
      requestId: req.id
    });
  });

  app.use(async (req, res, next) => {
    if (req.path === '/healthz') {
      next();
      return;
    }

    await sleep(randomInt(latency.min, latency.max));

    if (chance(failureRate)) {
      req.log.warn({ path: req.path }, 'Synthetic upstream failure triggered');
      res.status(503).json({
        error: `${serviceName} temporary failure`,
        requestId: req.id
      });
      return;
    }

    next();
  });

  registerRoutes(app, logger);

  const port = Number(process.env.PORT || defaultPort);

  const server = app.listen(port, () => {
    logger.info({ port }, `${serviceName} listening`);
  });

  async function stop(signal) {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    logger.info({ signal }, 'Starting graceful shutdown');

    await new Promise((resolve) => {
      server.close(() => resolve());
    });

    const shutdownDeadline = Date.now() + 10_000;
    while (inFlight > 0 && Date.now() < shutdownDeadline) {
      await sleep(50);
    }

    await shutdownTracing(sdk, logger);
    logger.info({ inFlight }, 'Domain service stopped');
    process.exit(0);
  }

  process.on('SIGINT', () => {
    stop('SIGINT');
  });

  process.on('SIGTERM', () => {
    stop('SIGTERM');
  });

  return { app, server, logger };
}

module.exports = {
  createDomainService
};
