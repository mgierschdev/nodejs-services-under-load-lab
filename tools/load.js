#!/usr/bin/env node

const { randomUUID } = require('node:crypto');
const { fetch } = require('undici');

function parseArgs(argv) {
  const defaults = {
    target: 'http://localhost:3000',
    duration: 30,
    concurrency: 50,
    mode: 'deals'
  };

  const options = { ...defaults };

  for (const arg of argv) {
    const [rawKey, rawValue] = arg.split('=');
    if (!rawKey.startsWith('--')) {
      continue;
    }

    const key = rawKey.slice(2);
    const value = rawValue ?? '';

    if (key === 'target' && value) {
      options.target = value.replace(/\/$/, '');
    }

    if (key === 'duration') {
      options.duration = Number(value) || defaults.duration;
    }

    if (key === 'concurrency') {
      options.concurrency = Number(value) || defaults.concurrency;
    }

    if (key === 'mode' && value) {
      options.mode = value;
    }
  }

  return options;
}

function chooseMode(mode) {
  if (mode !== 'mixed') {
    return mode;
  }

  const roll = Math.random();

  if (roll < 0.68) {
    return 'deals';
  }

  if (roll < 0.84) {
    return 'enqueue';
  }

  if (roll < 0.92) {
    return 'hash-bad';
  }

  return 'hash-good';
}

function buildRequest(target, mode) {
  if (mode === 'deals') {
    return {
      url: `${target}/api/deals?city=valencia&limit=20`,
      method: 'GET',
      body: null,
      timeoutMs: 5000
    };
  }

  if (mode === 'enqueue') {
    return {
      url: `${target}/api/enrichment-jobs`,
      method: 'POST',
      body: { count: 200, city: 'valencia' },
      timeoutMs: 2000
    };
  }

  if (mode === 'hash-bad') {
    return {
      url: `${target}/api/heavy-hash/bad`,
      method: 'POST',
      body: { rounds: 2, iterations: 180000 },
      timeoutMs: 7000
    };
  }

  if (mode === 'hash-good') {
    return {
      url: `${target}/api/heavy-hash/good`,
      method: 'POST',
      body: { rounds: 2, iterations: 180000 },
      timeoutMs: 7000
    };
  }

  throw new Error(`Unsupported mode: ${mode}`);
}

function percentile(values, p) {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[index];
}

async function run() {
  const options = parseArgs(process.argv.slice(2));

  const startedAt = Date.now();
  const deadline = startedAt + options.duration * 1000;

  const stats = {
    sent: 0,
    success: 0,
    failures: 0,
    latenciesMs: []
  };

  async function workerLoop() {
    while (Date.now() < deadline) {
      const mode = chooseMode(options.mode);
      const request = buildRequest(options.target, mode);
      const requestId = randomUUID();

      const requestStarted = Date.now();
      stats.sent += 1;

      try {
        const response = await fetch(request.url, {
          method: request.method,
          headers: {
            'content-type': 'application/json',
            'x-correlation-id': requestId
          },
          body: request.body ? JSON.stringify(request.body) : undefined,
          signal: AbortSignal.timeout(request.timeoutMs)
        });

        const latencyMs = Date.now() - requestStarted;
        stats.latenciesMs.push(latencyMs);

        if (response.ok || response.status === 202 || response.status === 429 || response.status === 409) {
          stats.success += 1;
        } else {
          stats.failures += 1;
        }
      } catch (_error) {
        stats.failures += 1;
        stats.latenciesMs.push(Date.now() - requestStarted);
      }
    }
  }

  await Promise.all(Array.from({ length: options.concurrency }).map(() => workerLoop()));

  const elapsedSeconds = Math.max((Date.now() - startedAt) / 1000, 1e-6);
  const avgLatency =
    stats.latenciesMs.length > 0
      ? stats.latenciesMs.reduce((acc, value) => acc + value, 0) / stats.latenciesMs.length
      : 0;
  const p95Latency = percentile(stats.latenciesMs, 0.95);
  const rps = stats.sent / elapsedSeconds;

  // eslint-disable-next-line no-console
  console.log(`target=${options.target}`);
  // eslint-disable-next-line no-console
  console.log(`mode=${options.mode}`);
  // eslint-disable-next-line no-console
  console.log(`durationSeconds=${options.duration}`);
  // eslint-disable-next-line no-console
  console.log(`concurrency=${options.concurrency}`);
  // eslint-disable-next-line no-console
  console.log(`requestsSent=${stats.sent}`);
  // eslint-disable-next-line no-console
  console.log(`success=${stats.success}`);
  // eslint-disable-next-line no-console
  console.log(`failures=${stats.failures}`);
  // eslint-disable-next-line no-console
  console.log(`rps=${rps.toFixed(2)}`);
  // eslint-disable-next-line no-console
  console.log(`avgLatencyMs=${avgLatency.toFixed(2)}`);
  // eslint-disable-next-line no-console
  console.log(`p95LatencyMs=${p95Latency.toFixed(2)}`);
}

run().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error.message);
  process.exit(1);
});
