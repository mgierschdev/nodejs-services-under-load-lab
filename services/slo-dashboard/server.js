const path = require('node:path');
const { randomUUID } = require('node:crypto');

const express = require('express');
const pinoHttp = require('pino-http');
const { fetch } = require('undici');

const { createLogger } = require('../../shared/logger');
const { startTracing, shutdownTracing } = require('../../shared/tracing');
const { sleep } = require('../../shared/utils');

const serviceName = 'slo-dashboard';
const sdk = startTracing(serviceName);
const logger = createLogger(serviceName);

const app = express();
const port = Number(process.env.PORT || 3100);
const metricsUrl = process.env.EXPERIENCE_METRICS_URL || 'http://localhost:3000/metrics';
const experienceApiUrl = process.env.EXPERIENCE_API_URL || 'http://localhost:3000';
const ratingLbStatsUrl = process.env.RATING_LB_STATS_URL || 'http://localhost:3014/stats';

const chartJsFile = path.join(process.cwd(), 'node_modules', 'chart.js', 'dist', 'chart.umd.js');

const SLO_TARGETS = {
  p95LatencyMs: 300,
  errorRatePct: 1,
  queueDepth: 1000
};

const LATENCY_THRESHOLDS_MS = {
  api: { ok: 600, warn: 1200 },
  upstream: { ok: 450, warn: 1000 },
  ratingLb: { ok: 500, warn: 1000 }
};

const HYBRID_ASSUMED_LIMIT = Math.max(Number(process.env.HYBRID_ASSUMED_LIMIT || 20), 1);
const HYBRID_ASSUMED_ENRICHMENTS_PER_DEAL = 4;

const CORE_DOMAIN_SERVICES = ['deal-service', 'price-service', 'inventory-service', 'merchant-service'];
const UPSTREAM_SERVICES = [...CORE_DOMAIN_SERVICES, 'rating-lb'];
const DEFAULT_RATING_TARGETS = ['rating-service-a', 'rating-service-b'];

const QUEUE_NODES = [
  {
    nodeId: 'enrichment-queue',
    queueLabel: 'enrichment-jobs'
  },
  {
    nodeId: 'critical-queue',
    queueLabel: 'critical-jobs'
  }
];

const INGRESS_METRIC_KEYS = [
  { key: 'dealsList', label: 'GET /api/deals', method: 'GET', route: '/api/deals' },
  { key: 'dealDetail', label: 'GET /api/deals/:id', method: 'GET', route: '/api/deals/:id' },
  { key: 'purchase', label: 'POST /api/purchase', method: 'POST', route: '/api/purchase' },
  {
    key: 'enqueue',
    label: 'POST /api/enrichment-jobs',
    method: 'POST',
    route: '/api/enrichment-jobs'
  },
  {
    key: 'hashBad',
    label: 'POST /api/heavy-hash/bad',
    method: 'POST',
    route: '/api/heavy-hash/bad'
  },
  {
    key: 'hashGood',
    label: 'POST /api/heavy-hash/good',
    method: 'POST',
    route: '/api/heavy-hash/good'
  }
];

const SEVERITY_RANK = {
  unknown: 0,
  ok: 1,
  warn: 2,
  critical: 3
};

const rateState = {
  lastTimestamp: 0,
  totalRequests: 0,
  totalErrors: 0,
  upstreamTotal: 0,
  upstreamFailures: 0,
  queueCompleted: 0
};

const topologyRateState = {
  lastTimestamp: 0,
  apiTotalRequests: 0,
  apiTotalErrors: 0,
  ingressByKey: Object.fromEntries(INGRESS_METRIC_KEYS.map((entry) => [entry.key, 0])),
  upstreamTotalsByService: Object.fromEntries(UPSTREAM_SERVICES.map((service) => [service, 0])),
  upstreamFailuresByService: Object.fromEntries(UPSTREAM_SERVICES.map((service) => [service, 0])),
  queueCompletedByNode: Object.fromEntries(QUEUE_NODES.map((queueNode) => [queueNode.nodeId, 0])),
  queueFailedByNode: Object.fromEntries(QUEUE_NODES.map((queueNode) => [queueNode.nodeId, 0])),
  ratingLbTotalRequests: 0,
  ratingLbTotalErrors: 0,
  ratingTargetTotals: Object.fromEntries(DEFAULT_RATING_TARGETS.map((targetId) => [targetId, 0])),
  ratingTargetErrors: Object.fromEntries(DEFAULT_RATING_TARGETS.map((targetId) => [targetId, 0]))
};

const hybridRateState = {
  lastTimestamp: 0,
  readModelHits: 0,
  readModelMisses: 0,
  readModelStale: 0,
  readModelLiveFallback: 0,
  readModelRefreshEnqueued: 0,
  catalogCacheHits: 0,
  catalogCacheMisses: 0,
  dealsListRequests: 0
};

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
    res.status(503).json({ error: 'Dashboard shutting down', requestId: req.id });
    return;
  }

  inFlight += 1;
  res.on('finish', () => {
    inFlight = Math.max(0, inFlight - 1);
  });

  next();
});

function parseLabels(input) {
  if (!input) {
    return {};
  }

  const labels = {};
  const regex = /(\w+)="([^"]*)"/g;
  let match = regex.exec(input);

  while (match) {
    labels[match[1]] = match[2];
    match = regex.exec(input);
  }

  return labels;
}

function parsePrometheusText(text) {
  const metrics = new Map();
  const lines = text.split('\n');

  for (const rawLine of lines) {
    const line = rawLine.trim();

    if (!line || line.startsWith('#')) {
      continue;
    }

    const parts = line.split(/\s+/);
    if (parts.length < 2) {
      continue;
    }

    const metricPart = parts[0];
    const value = Number(parts[parts.length - 1]);

    if (Number.isNaN(value)) {
      continue;
    }

    const match = metricPart.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?$/);
    if (!match) {
      continue;
    }

    const metricName = match[1];
    const labels = parseLabels(match[2]);
    const sample = { labels, value };

    if (!metrics.has(metricName)) {
      metrics.set(metricName, []);
    }

    metrics.get(metricName).push(sample);
  }

  return metrics;
}

function labelsMatch(labels, labelFilter) {
  if (!labelFilter) {
    return true;
  }

  return Object.entries(labelFilter).every(([key, expected]) => labels[key] === String(expected));
}

function sumMetric(metrics, metricName, labelFilter) {
  const samples = metrics.get(metricName) || [];

  return samples
    .filter((sample) => labelsMatch(sample.labels, labelFilter))
    .reduce((acc, sample) => acc + sample.value, 0);
}

function histogramQuantile(metrics, baseMetricName, quantile, labelFilter) {
  const bucketSamples = (metrics.get(`${baseMetricName}_bucket`) || []).filter((sample) =>
    labelsMatch(sample.labels, labelFilter)
  );
  const count = sumMetric(metrics, `${baseMetricName}_count`, labelFilter);

  if (count <= 0 || bucketSamples.length === 0) {
    return 0;
  }

  const groupedByLe = new Map();

  for (const sample of bucketSamples) {
    const leRaw = sample.labels.le;
    const leValue = leRaw === '+Inf' ? Number.POSITIVE_INFINITY : Number(leRaw);

    if (Number.isNaN(leValue)) {
      continue;
    }

    groupedByLe.set(leValue, (groupedByLe.get(leValue) || 0) + sample.value);
  }

  const sorted = [...groupedByLe.entries()].sort((a, b) => a[0] - b[0]);
  const target = count * quantile;

  let previousUpperBound = 0;
  let previousCumulativeCount = 0;

  for (const [upperBound, cumulativeCount] of sorted) {
    if (cumulativeCount >= target) {
      if (!Number.isFinite(upperBound)) {
        return Number.isFinite(previousUpperBound) ? previousUpperBound : 0;
      }

      const bucketCount = cumulativeCount - previousCumulativeCount;
      if (bucketCount <= 0) {
        return upperBound;
      }

      const bucketPosition = (target - previousCumulativeCount) / bucketCount;
      const interpolated =
        previousUpperBound + (upperBound - previousUpperBound) * Math.min(Math.max(bucketPosition, 0), 1);

      return interpolated;
    }

    previousUpperBound = upperBound;
    previousCumulativeCount = cumulativeCount;
  }

  const last = sorted[sorted.length - 1];
  return Number.isFinite(last[0]) ? last[0] : Number.isFinite(previousUpperBound) ? previousUpperBound : 0;
}

function round(value, digits = 2) {
  if (value === Number.POSITIVE_INFINITY) {
    return 'Infinity';
  }

  if (value === Number.NEGATIVE_INFINITY) {
    return '-Infinity';
  }

  if (!Number.isFinite(value)) {
    return value;
  }

  return Number(value.toFixed(digits));
}

function maxSeverity(severities) {
  return severities.reduce((highest, current) => {
    if ((SEVERITY_RANK[current] || 0) > (SEVERITY_RANK[highest] || 0)) {
      return current;
    }

    return highest;
  }, 'unknown');
}

function latencySeverity(latencyMs, profile = 'api') {
  if (!Number.isFinite(latencyMs)) {
    return 'unknown';
  }

  const thresholds = LATENCY_THRESHOLDS_MS[profile] || LATENCY_THRESHOLDS_MS.api;

  if (latencyMs < thresholds.ok) {
    return 'ok';
  }

  if (latencyMs <= thresholds.warn) {
    return 'warn';
  }

  return 'critical';
}

function rateSeverity(ratePct) {
  if (!Number.isFinite(ratePct)) {
    return 'unknown';
  }

  if (ratePct < 1) {
    return 'ok';
  }

  if (ratePct <= 3) {
    return 'warn';
  }

  return 'critical';
}

function failureSeverity(ratePct, requestRateRps) {
  if (!Number.isFinite(ratePct)) {
    return 'unknown';
  }

  if (Number.isFinite(requestRateRps) && requestRateRps < 1) {
    return ratePct > 0 ? 'warn' : 'ok';
  }

  return rateSeverity(ratePct);
}

function queueDepthSeverity(depth) {
  if (!Number.isFinite(depth)) {
    return 'unknown';
  }

  if (depth < 300) {
    return 'ok';
  }

  if (depth <= 1000) {
    return 'warn';
  }

  return 'critical';
}

function queueLagSeverity(lagSeconds, depth = 0) {
  if (!Number.isFinite(lagSeconds)) {
    if (lagSeconds !== Number.POSITIVE_INFINITY) {
      return 'unknown';
    }

    if (depth <= 0) {
      return 'ok';
    }

    if (depth < 10) {
      return 'warn';
    }

    return 'critical';
  }

  if (lagSeconds < 5) {
    return 'ok';
  }

  if (lagSeconds <= 20) {
    return 'warn';
  }

  return 'critical';
}

function toNodeStatus(severity, readiness) {
  if (readiness === false) {
    return 'down';
  }

  if (severity === 'critical' || severity === 'warn') {
    return 'degraded';
  }

  if (severity === 'ok') {
    return 'up';
  }

  if (readiness === true) {
    return 'up';
  }

  return 'unknown';
}

function getDependencyState(readySnapshot, dependencyName) {
  const check = readySnapshot?.checks?.find((entry) => entry.dependency === dependencyName);

  if (!check) {
    return null;
  }

  return Boolean(check.ok);
}

function buildTopologyEdges(ratingTargetIds) {
  return [
    { id: 'client->experience', from: 'client-ingress', to: 'experience-api', kind: 'ingress' },
    { id: 'experience->deal', from: 'experience-api', to: 'deal-service', kind: 'service' },
    { id: 'experience->price', from: 'experience-api', to: 'price-service', kind: 'service' },
    { id: 'experience->inventory', from: 'experience-api', to: 'inventory-service', kind: 'service' },
    { id: 'experience->rating-lb', from: 'experience-api', to: 'rating-lb', kind: 'service' },
    { id: 'experience->merchant', from: 'experience-api', to: 'merchant-service', kind: 'service' },
    {
      id: 'experience->enrichment-queue',
      from: 'experience-api',
      to: 'enrichment-queue',
      kind: 'queue'
    },
    {
      id: 'experience->critical-queue',
      from: 'experience-api',
      to: 'critical-queue',
      kind: 'queue'
    },
    { id: 'experience->redis', from: 'experience-api', to: 'redis', kind: 'cache' },
    {
      id: 'enrichment-queue->enrichment-worker',
      from: 'enrichment-queue',
      to: 'enrichment-worker',
      kind: 'worker'
    },
    {
      id: 'critical-queue->critical-worker',
      from: 'critical-queue',
      to: 'critical-worker',
      kind: 'worker'
    },
    ...ratingTargetIds.map((targetId) => ({
      id: `rating-lb->${targetId}`,
      from: 'rating-lb',
      to: targetId,
      kind: 'lb-target'
    })),
    { id: 'redis->enrichment-queue', from: 'redis', to: 'enrichment-queue', kind: 'redis' },
    { id: 'redis->critical-queue', from: 'redis', to: 'critical-queue', kind: 'redis' }
  ];
}

async function fetchMetricsSnapshot(requestId) {
  const response = await fetch(metricsUrl, {
    headers: {
      'x-correlation-id': requestId
    },
    signal: AbortSignal.timeout(1200)
  });

  if (!response.ok) {
    throw new Error(`Failed to scrape metrics (${response.status})`);
  }

  return response.text();
}

async function fetchReadySnapshot(requestId) {
  try {
    const response = await fetch(`${experienceApiUrl}/readyz`, {
      headers: {
        'x-correlation-id': requestId
      },
      signal: AbortSignal.timeout(1200)
    });

    if (!response.ok) {
      return null;
    }

    return response.json();
  } catch (_error) {
    return null;
  }
}

async function fetchRatingLbStats(requestId) {
  try {
    const response = await fetch(ratingLbStatsUrl, {
      headers: {
        'x-correlation-id': requestId
      },
      signal: AbortSignal.timeout(1200)
    });

    if (!response.ok) {
      return null;
    }

    const payload = await response.json();
    if (!Array.isArray(payload?.targets)) {
      return null;
    }

    return payload;
  } catch (_error) {
    return null;
  }
}

function computeRates(current) {
  const now = Date.now();

  if (!rateState.lastTimestamp) {
    rateState.lastTimestamp = now;
    rateState.totalRequests = current.totalRequests;
    rateState.totalErrors = current.totalErrors;
    rateState.upstreamTotal = current.upstreamTotal;
    rateState.upstreamFailures = current.upstreamFailures;
    rateState.queueCompleted = current.queueCompleted;

    return {
      requestRate: 0,
      errorRateDeltaPct: 0,
      upstreamFailureRateDeltaPct: 0,
      queueCompletionRatePerSecond: 0
    };
  }

  const deltaSeconds = Math.max((now - rateState.lastTimestamp) / 1000, 1e-6);

  const deltaRequests = Math.max(current.totalRequests - rateState.totalRequests, 0);
  const deltaErrors = Math.max(current.totalErrors - rateState.totalErrors, 0);
  const deltaUpstreamTotal = Math.max(current.upstreamTotal - rateState.upstreamTotal, 0);
  const deltaUpstreamFailures = Math.max(current.upstreamFailures - rateState.upstreamFailures, 0);
  const deltaQueueCompleted = Math.max(current.queueCompleted - rateState.queueCompleted, 0);

  const result = {
    requestRate: deltaRequests / deltaSeconds,
    errorRateDeltaPct: deltaRequests > 0 ? (deltaErrors / deltaRequests) * 100 : 0,
    upstreamFailureRateDeltaPct:
      deltaUpstreamTotal > 0 ? (deltaUpstreamFailures / deltaUpstreamTotal) * 100 : 0,
    queueCompletionRatePerSecond: deltaQueueCompleted / deltaSeconds
  };

  rateState.lastTimestamp = now;
  rateState.totalRequests = current.totalRequests;
  rateState.totalErrors = current.totalErrors;
  rateState.upstreamTotal = current.upstreamTotal;
  rateState.upstreamFailures = current.upstreamFailures;
  rateState.queueCompleted = current.queueCompleted;

  return result;
}

function computeHybridRates(current) {
  const now = Date.now();

  if (!hybridRateState.lastTimestamp) {
    hybridRateState.lastTimestamp = now;
    hybridRateState.readModelHits = current.readModelHits;
    hybridRateState.readModelMisses = current.readModelMisses;
    hybridRateState.readModelStale = current.readModelStale;
    hybridRateState.readModelLiveFallback = current.readModelLiveFallback;
    hybridRateState.readModelRefreshEnqueued = current.readModelRefreshEnqueued;
    hybridRateState.catalogCacheHits = current.catalogCacheHits;
    hybridRateState.catalogCacheMisses = current.catalogCacheMisses;
    hybridRateState.dealsListRequests = current.dealsListRequests;

    return {
      readModelHitRps: 0,
      readModelMissRps: 0,
      readModelStaleRps: 0,
      liveFallbackRps: 0,
      refreshEnqueueRps: 0,
      catalogHitRps: 0,
      catalogMissRps: 0,
      dealsListRps: 0
    };
  }

  const deltaSeconds = Math.max((now - hybridRateState.lastTimestamp) / 1000, 1e-6);

  const deltas = {
    readModelHits: Math.max(current.readModelHits - hybridRateState.readModelHits, 0),
    readModelMisses: Math.max(current.readModelMisses - hybridRateState.readModelMisses, 0),
    readModelStale: Math.max(current.readModelStale - hybridRateState.readModelStale, 0),
    readModelLiveFallback: Math.max(current.readModelLiveFallback - hybridRateState.readModelLiveFallback, 0),
    readModelRefreshEnqueued: Math.max(
      current.readModelRefreshEnqueued - hybridRateState.readModelRefreshEnqueued,
      0
    ),
    catalogCacheHits: Math.max(current.catalogCacheHits - hybridRateState.catalogCacheHits, 0),
    catalogCacheMisses: Math.max(current.catalogCacheMisses - hybridRateState.catalogCacheMisses, 0),
    dealsListRequests: Math.max(current.dealsListRequests - hybridRateState.dealsListRequests, 0)
  };

  hybridRateState.lastTimestamp = now;
  hybridRateState.readModelHits = current.readModelHits;
  hybridRateState.readModelMisses = current.readModelMisses;
  hybridRateState.readModelStale = current.readModelStale;
  hybridRateState.readModelLiveFallback = current.readModelLiveFallback;
  hybridRateState.readModelRefreshEnqueued = current.readModelRefreshEnqueued;
  hybridRateState.catalogCacheHits = current.catalogCacheHits;
  hybridRateState.catalogCacheMisses = current.catalogCacheMisses;
  hybridRateState.dealsListRequests = current.dealsListRequests;

  return {
    readModelHitRps: deltas.readModelHits / deltaSeconds,
    readModelMissRps: deltas.readModelMisses / deltaSeconds,
    readModelStaleRps: deltas.readModelStale / deltaSeconds,
    liveFallbackRps: deltas.readModelLiveFallback / deltaSeconds,
    refreshEnqueueRps: deltas.readModelRefreshEnqueued / deltaSeconds,
    catalogHitRps: deltas.catalogCacheHits / deltaSeconds,
    catalogMissRps: deltas.catalogCacheMisses / deltaSeconds,
    dealsListRps: deltas.dealsListRequests / deltaSeconds
  };
}

function computeTopologyRates(current) {
  const now = Date.now();

  if (!topologyRateState.lastTimestamp) {
    topologyRateState.lastTimestamp = now;
    topologyRateState.apiTotalRequests = current.apiTotalRequests;
    topologyRateState.apiTotalErrors = current.apiTotalErrors;
    topologyRateState.ingressByKey = { ...current.ingressByKey };
    topologyRateState.upstreamTotalsByService = { ...current.upstreamTotalsByService };
    topologyRateState.upstreamFailuresByService = { ...current.upstreamFailuresByService };
    topologyRateState.queueCompletedByNode = { ...current.queueCompletedByNode };
    topologyRateState.queueFailedByNode = { ...current.queueFailedByNode };
    topologyRateState.ratingLbTotalRequests = current.ratingLbTotalRequests;
    topologyRateState.ratingLbTotalErrors = current.ratingLbTotalErrors;
    topologyRateState.ratingTargetTotals = { ...current.ratingTargetTotals };
    topologyRateState.ratingTargetErrors = { ...current.ratingTargetErrors };

    return {
      apiRequestRate: 0,
      apiErrorRatePct:
        current.apiTotalRequests > 0 ? (current.apiTotalErrors / current.apiTotalRequests) * 100 : 0,
      ingressRequestRates: Object.fromEntries(INGRESS_METRIC_KEYS.map((entry) => [entry.key, 0])),
      serviceRequestRates: Object.fromEntries(UPSTREAM_SERVICES.map((service) => [service, 0])),
      serviceFailureRates: Object.fromEntries(UPSTREAM_SERVICES.map((service) => [service, 0])),
      queueCompletionRates: Object.fromEntries(QUEUE_NODES.map((queueNode) => [queueNode.nodeId, 0])),
      queueFailureRates: Object.fromEntries(QUEUE_NODES.map((queueNode) => [queueNode.nodeId, 0])),
      ratingLbRequestRate: 0,
      ratingLbFailureRatePct:
        current.ratingLbTotalRequests > 0
          ? (current.ratingLbTotalErrors / current.ratingLbTotalRequests) * 100
          : 0,
      ratingTargetRequestRates: Object.fromEntries(
        Object.keys(current.ratingTargetTotals).map((targetId) => [targetId, 0])
      ),
      ratingTargetFailureRates: Object.fromEntries(
        Object.keys(current.ratingTargetTotals).map((targetId) => [targetId, 0])
      )
    };
  }

  const deltaSeconds = Math.max((now - topologyRateState.lastTimestamp) / 1000, 1e-6);

  const deltaApiRequests = Math.max(current.apiTotalRequests - topologyRateState.apiTotalRequests, 0);
  const deltaApiErrors = Math.max(current.apiTotalErrors - topologyRateState.apiTotalErrors, 0);

  const ingressRequestRates = {};
  for (const entry of INGRESS_METRIC_KEYS) {
    const currentTotal = current.ingressByKey[entry.key] || 0;
    const previousTotal = topologyRateState.ingressByKey[entry.key] || 0;
    ingressRequestRates[entry.key] = Math.max(currentTotal - previousTotal, 0) / deltaSeconds;
  }

  const serviceRequestRates = {};
  const serviceFailureRates = {};

  for (const service of UPSTREAM_SERVICES) {
    const currentTotal = current.upstreamTotalsByService[service] || 0;
    const currentFailures = current.upstreamFailuresByService[service] || 0;

    const previousTotal = topologyRateState.upstreamTotalsByService[service] || 0;
    const previousFailures = topologyRateState.upstreamFailuresByService[service] || 0;

    const deltaTotal = Math.max(currentTotal - previousTotal, 0);
    const deltaFailures = Math.max(currentFailures - previousFailures, 0);

    serviceRequestRates[service] = deltaTotal / deltaSeconds;

    if (deltaTotal > 0) {
      serviceFailureRates[service] = (deltaFailures / deltaTotal) * 100;
    } else if (currentTotal > 0) {
      serviceFailureRates[service] = (currentFailures / currentTotal) * 100;
    } else {
      serviceFailureRates[service] = 0;
    }
  }

  const queueCompletionRates = {};
  const queueFailureRates = {};

  for (const queueNode of QUEUE_NODES) {
    const nodeId = queueNode.nodeId;

    const currentCompleted = current.queueCompletedByNode[nodeId] || 0;
    const currentFailed = current.queueFailedByNode[nodeId] || 0;

    const previousCompleted = topologyRateState.queueCompletedByNode[nodeId] || 0;
    const previousFailed = topologyRateState.queueFailedByNode[nodeId] || 0;

    const deltaCompleted = Math.max(currentCompleted - previousCompleted, 0);
    const deltaFailed = Math.max(currentFailed - previousFailed, 0);

    queueCompletionRates[nodeId] = deltaCompleted / deltaSeconds;

    if (deltaCompleted + deltaFailed > 0) {
      queueFailureRates[nodeId] = (deltaFailed / (deltaCompleted + deltaFailed)) * 100;
    } else if (currentCompleted + currentFailed > 0) {
      queueFailureRates[nodeId] = (currentFailed / (currentCompleted + currentFailed)) * 100;
    } else {
      queueFailureRates[nodeId] = 0;
    }
  }

  const deltaLbRequests = Math.max(
    current.ratingLbTotalRequests - topologyRateState.ratingLbTotalRequests,
    0
  );
  const deltaLbErrors = Math.max(current.ratingLbTotalErrors - topologyRateState.ratingLbTotalErrors, 0);

  const ratingTargetRequestRates = {};
  const ratingTargetFailureRates = {};

  for (const targetId of Object.keys(current.ratingTargetTotals)) {
    const currentRequests = current.ratingTargetTotals[targetId] || 0;
    const currentErrors = current.ratingTargetErrors[targetId] || 0;

    const previousRequests = topologyRateState.ratingTargetTotals[targetId] || 0;
    const previousErrors = topologyRateState.ratingTargetErrors[targetId] || 0;

    const deltaRequests = Math.max(currentRequests - previousRequests, 0);
    const deltaErrors = Math.max(currentErrors - previousErrors, 0);

    ratingTargetRequestRates[targetId] = deltaRequests / deltaSeconds;

    if (deltaRequests > 0) {
      ratingTargetFailureRates[targetId] = (deltaErrors / deltaRequests) * 100;
    } else if (currentRequests > 0) {
      ratingTargetFailureRates[targetId] = (currentErrors / currentRequests) * 100;
    } else {
      ratingTargetFailureRates[targetId] = 0;
    }
  }

  topologyRateState.lastTimestamp = now;
  topologyRateState.apiTotalRequests = current.apiTotalRequests;
  topologyRateState.apiTotalErrors = current.apiTotalErrors;
  topologyRateState.ingressByKey = { ...current.ingressByKey };
  topologyRateState.upstreamTotalsByService = { ...current.upstreamTotalsByService };
  topologyRateState.upstreamFailuresByService = { ...current.upstreamFailuresByService };
  topologyRateState.queueCompletedByNode = { ...current.queueCompletedByNode };
  topologyRateState.queueFailedByNode = { ...current.queueFailedByNode };
  topologyRateState.ratingLbTotalRequests = current.ratingLbTotalRequests;
  topologyRateState.ratingLbTotalErrors = current.ratingLbTotalErrors;
  topologyRateState.ratingTargetTotals = { ...current.ratingTargetTotals };
  topologyRateState.ratingTargetErrors = { ...current.ratingTargetErrors };

  return {
    apiRequestRate: deltaApiRequests / deltaSeconds,
    apiErrorRatePct: deltaApiRequests > 0 ? (deltaApiErrors / deltaApiRequests) * 100 : 0,
    ingressRequestRates,
    serviceRequestRates,
    serviceFailureRates,
    queueCompletionRates,
    queueFailureRates,
    ratingLbRequestRate: deltaLbRequests / deltaSeconds,
    ratingLbFailureRatePct:
      deltaLbRequests > 0
        ? (deltaLbErrors / deltaLbRequests) * 100
        : current.ratingLbTotalRequests > 0
        ? (current.ratingLbTotalErrors / current.ratingLbTotalRequests) * 100
        : 0,
    ratingTargetRequestRates,
    ratingTargetFailureRates
  };
}

function ratingTargetsFromStats(ratingLbStats) {
  if (!ratingLbStats || !Array.isArray(ratingLbStats.targets) || ratingLbStats.targets.length === 0) {
    return DEFAULT_RATING_TARGETS.map((targetId) => ({
      id: targetId,
      totalRequests: 0,
      totalErrors: 0,
      healthy: null
    }));
  }

  return ratingLbStats.targets.map((target) => ({
    id: target.id,
    totalRequests: Number(target.totalRequests || 0),
    totalErrors: Number(target.totalErrors || 0),
    healthy: typeof target.healthy === 'boolean' ? target.healthy : null,
    lastStatusCode: target.lastStatusCode,
    lastError: target.lastError || null,
    url: target.url || null
  }));
}

function createUnknownTopology(readySnapshot, ratingLbStats) {
  const ratingTargets = ratingTargetsFromStats(ratingLbStats);
  const ratingTargetIds = ratingTargets.map((target) => target.id);
  const topologyEdges = buildTopologyEdges(ratingTargetIds);

  const apiReadiness = readySnapshot
    ? readySnapshot.status === 'ready'
      ? true
      : false
    : null;

  const redisReady = getDependencyState(readySnapshot, 'redis');
  const redisSeverity = redisReady === false ? 'critical' : 'unknown';
  const apiSeverity = apiReadiness === false ? 'critical' : readySnapshot ? 'ok' : 'unknown';
  const ratingLbReady = getDependencyState(readySnapshot, 'rating-lb');

  const nodes = [
    {
      id: 'client-ingress',
      type: 'client',
      label: 'clients',
      severity: apiReadiness === false ? 'critical' : 'unknown',
      status: toNodeStatus(apiReadiness === false ? 'critical' : 'unknown', apiReadiness),
      metrics: {
        totalIngressRps: 0
      }
    },
    {
      id: 'experience-api',
      type: 'api',
      label: 'experience-api',
      severity: apiSeverity,
      status: toNodeStatus(apiSeverity, apiReadiness),
      metrics: {}
    },
    ...CORE_DOMAIN_SERVICES.map((service) => {
      const ready = getDependencyState(readySnapshot, service);
      const severity = ready === false ? 'critical' : 'unknown';
      return {
        id: service,
        type: 'service',
        label: service,
        severity,
        status: toNodeStatus(severity, ready),
        metrics: {}
      };
    }),
    {
      id: 'rating-lb',
      type: 'service',
      label: 'rating-lb',
      severity: ratingLbReady === false ? 'critical' : 'unknown',
      status: toNodeStatus(ratingLbReady === false ? 'critical' : 'unknown', ratingLbReady),
      metrics: {}
    },
    ...ratingTargets.map((target) => {
      const severity = target.healthy === false ? 'critical' : 'unknown';
      return {
        id: target.id,
        type: 'service',
        label: target.id,
        severity,
        status: toNodeStatus(severity, target.healthy),
        metrics: {}
      };
    }),
    {
      id: 'enrichment-queue',
      type: 'queue',
      label: 'enrichment-queue',
      severity: redisReady === false ? 'critical' : 'unknown',
      status: toNodeStatus(redisReady === false ? 'critical' : 'unknown', redisReady),
      metrics: {}
    },
    {
      id: 'critical-queue',
      type: 'queue',
      label: 'critical-queue',
      severity: redisReady === false ? 'critical' : 'unknown',
      status: toNodeStatus(redisReady === false ? 'critical' : 'unknown', redisReady),
      metrics: {}
    },
    {
      id: 'enrichment-worker',
      type: 'worker',
      label: 'enrichment-worker',
      severity: redisReady === false ? 'critical' : 'unknown',
      status: toNodeStatus(redisReady === false ? 'critical' : 'unknown', redisReady),
      metrics: {}
    },
    {
      id: 'critical-worker',
      type: 'worker',
      label: 'critical-worker',
      severity: redisReady === false ? 'critical' : 'unknown',
      status: toNodeStatus(redisReady === false ? 'critical' : 'unknown', redisReady),
      metrics: {}
    },
    {
      id: 'redis',
      type: 'infra',
      label: 'redis',
      severity: redisSeverity,
      status: toNodeStatus(redisSeverity, redisReady),
      metrics: {
        ready: redisReady
      }
    }
  ];

  const nodeById = Object.fromEntries(nodes.map((node) => [node.id, node]));

  const edges = topologyEdges.map((edge) => ({
    id: edge.id,
    from: edge.from,
    to: edge.to,
    severity: maxSeverity([nodeById[edge.from]?.severity || 'unknown', nodeById[edge.to]?.severity || 'unknown']),
    metrics: {}
  }));

  return {
    timestamp: new Date().toISOString(),
    ingress: {
      totalRps: 0,
      byType: Object.fromEntries(INGRESS_METRIC_KEYS.map((entry) => [entry.key, 0]))
    },
    nodes,
    edges
  };
}

async function buildTopologyResponse(requestId) {
  const [readySnapshot, ratingLbStats] = await Promise.all([
    fetchReadySnapshot(requestId),
    fetchRatingLbStats(requestId)
  ]);

  let metrics;
  try {
    const metricsText = await fetchMetricsSnapshot(requestId);
    metrics = parsePrometheusText(metricsText);
  } catch (_error) {
    return createUnknownTopology(readySnapshot, ratingLbStats);
  }

  const ratingTargets = ratingTargetsFromStats(ratingLbStats);
  const ratingTargetIds = ratingTargets.map((target) => target.id);
  const topologyEdges = buildTopologyEdges(ratingTargetIds);

  const apiP95LatencyMs = histogramQuantile(metrics, 'experience_http_request_duration_seconds', 0.95) * 1000;
  const apiTotalRequests = sumMetric(metrics, 'experience_http_request_duration_seconds_count');
  const apiTotalErrors = sumMetric(metrics, 'experience_http_request_errors_total');

  const ingressByKey = Object.fromEntries(
    INGRESS_METRIC_KEYS.map((entry) => [
      entry.key,
      sumMetric(metrics, 'experience_http_request_duration_seconds_count', {
        method: entry.method,
        route: entry.route
      })
    ])
  );

  const upstreamTotalsByService = Object.fromEntries(
    UPSTREAM_SERVICES.map((service) => [
      service,
      sumMetric(metrics, 'experience_upstream_requests_total', { service })
    ])
  );

  const upstreamFailuresByService = Object.fromEntries(
    UPSTREAM_SERVICES.map((service) => [
      service,
      sumMetric(metrics, 'experience_upstream_failures_total', { service })
    ])
  );

  const queueDepthByNode = Object.fromEntries(
    QUEUE_NODES.map((queueNode) => [
      queueNode.nodeId,
      sumMetric(metrics, 'experience_queue_depth', { queue: queueNode.queueLabel })
    ])
  );

  const queueCompletedByNode = Object.fromEntries(
    QUEUE_NODES.map((queueNode) => [
      queueNode.nodeId,
      sumMetric(metrics, 'experience_queue_jobs_completed_total', { queue: queueNode.queueLabel })
    ])
  );

  const queueFailedByNode = Object.fromEntries(
    QUEUE_NODES.map((queueNode) => [
      queueNode.nodeId,
      sumMetric(metrics, 'experience_queue_jobs_failed_total', { queue: queueNode.queueLabel })
    ])
  );

  const ratingLbTotalRequests = Number(ratingLbStats?.totalRequests || 0);
  const ratingLbTotalErrors = Number(ratingLbStats?.totalErrors || 0);

  const ratingTargetTotals = Object.fromEntries(
    ratingTargets.map((target) => [target.id, Number(target.totalRequests || 0)])
  );
  const ratingTargetErrors = Object.fromEntries(
    ratingTargets.map((target) => [target.id, Number(target.totalErrors || 0)])
  );

  const rateSnapshot = computeTopologyRates({
    apiTotalRequests,
    apiTotalErrors,
    ingressByKey,
    upstreamTotalsByService,
    upstreamFailuresByService,
    queueCompletedByNode,
    queueFailedByNode,
    ratingLbTotalRequests,
    ratingLbTotalErrors,
    ratingTargetTotals,
    ratingTargetErrors
  });

  const readModelHits = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'hit' });
  const readModelMisses = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'miss' });
  const readModelStale = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'stale' });
  const catalogCacheHits = sumMetric(metrics, 'experience_catalog_cache_outcomes_total', { outcome: 'hit' });
  const catalogCacheMisses = sumMetric(metrics, 'experience_catalog_cache_outcomes_total', {
    outcome: 'miss'
  });

  const readModelLookups = readModelHits + readModelMisses + readModelStale;
  const readModelHitPct = readModelLookups > 0 ? (readModelHits / readModelLookups) * 100 : 0;
  const catalogTotal = catalogCacheHits + catalogCacheMisses;
  const catalogHitPct = catalogTotal > 0 ? (catalogCacheHits / catalogTotal) * 100 : 0;

  const queueMetricsByNode = {};
  for (const queueNode of QUEUE_NODES) {
    const nodeId = queueNode.nodeId;
    const depth = queueDepthByNode[nodeId] || 0;
    const completionRate = rateSnapshot.queueCompletionRates[nodeId] || 0;

    const lagEstimateSec =
      completionRate > 0
        ? depth / completionRate
        : depth > 0
        ? Number.POSITIVE_INFINITY
        : 0;

    queueMetricsByNode[nodeId] = {
      depth,
      completionRate,
      failureRatePct: rateSnapshot.queueFailureRates[nodeId] || 0,
      lagEstimateSec
    };
  }

  const apiReadiness = readySnapshot
    ? readySnapshot.status === 'ready'
      ? true
      : false
    : null;

  const ingressTotalRps = Object.values(rateSnapshot.ingressRequestRates).reduce(
    (acc, value) => acc + value,
    0
  );

  const apiBaseSeverity = maxSeverity([
    latencySeverity(apiP95LatencyMs, 'api'),
    rateSeverity(rateSnapshot.apiErrorRatePct)
  ]);

  const apiSeverity = apiReadiness === false ? 'critical' : apiBaseSeverity;
  const clientSeverity = apiReadiness === false ? 'critical' : apiSeverity === 'critical' ? 'warn' : 'ok';

  const nodes = [];

  nodes.push({
    id: 'client-ingress',
    type: 'client',
    label: 'clients',
    severity: clientSeverity,
    status: toNodeStatus(clientSeverity, apiReadiness),
    metrics: {
      totalIngressRps: round(ingressTotalRps, 2),
      requestTypeRps: Object.fromEntries(
        INGRESS_METRIC_KEYS.map((entry) => [entry.key, round(rateSnapshot.ingressRequestRates[entry.key] || 0, 2)])
      )
    }
  });

  nodes.push({
    id: 'experience-api',
    type: 'api',
    label: 'experience-api',
    severity: apiSeverity,
    status: toNodeStatus(apiSeverity, apiReadiness),
    metrics: {
      p95LatencyMs: round(apiP95LatencyMs, 1),
      errorRatePct: round(rateSnapshot.apiErrorRatePct, 3),
      requestRateRps: round(rateSnapshot.apiRequestRate, 2)
    }
  });

  for (const service of CORE_DOMAIN_SERVICES) {
    const p95LatencyMs =
      histogramQuantile(metrics, 'experience_upstream_request_duration_seconds', 0.95, { service }) * 1000;
    const failureRatePct = rateSnapshot.serviceFailureRates[service] || 0;
    const requestRateRps = rateSnapshot.serviceRequestRates[service] || 0;

    const readiness = getDependencyState(readySnapshot, service);

    const baseSeverity = maxSeverity([
      latencySeverity(p95LatencyMs, 'upstream'),
      failureSeverity(failureRatePct, requestRateRps)
    ]);

    const severity = readiness === false ? 'critical' : baseSeverity;

    nodes.push({
      id: service,
      type: 'service',
      label: service,
      severity,
      status: toNodeStatus(severity, readiness),
      metrics: {
        p95LatencyMs: round(p95LatencyMs, 1),
        failureRatePct: round(failureRatePct, 3),
        requestRateRps: round(requestRateRps, 2)
      }
    });
  }

  const ratingLbReadiness = getDependencyState(readySnapshot, 'rating-lb');
  const ratingLbP95LatencyMs =
    histogramQuantile(metrics, 'experience_upstream_request_duration_seconds', 0.95, {
      service: 'rating-lb'
    }) * 1000;
  const ratingLbFailureRatePct = rateSnapshot.serviceFailureRates['rating-lb'] || 0;
  const ratingLbRequestRateRps = rateSnapshot.serviceRequestRates['rating-lb'] || 0;

  const ratingLbBaseSeverity = maxSeverity([
    latencySeverity(ratingLbP95LatencyMs, 'ratingLb'),
    failureSeverity(ratingLbFailureRatePct, ratingLbRequestRateRps)
  ]);
  const ratingLbSeverity = ratingLbReadiness === false ? 'critical' : ratingLbBaseSeverity;

  nodes.push({
    id: 'rating-lb',
    type: 'service',
    label: 'rating-lb',
    severity: ratingLbSeverity,
    status: toNodeStatus(ratingLbSeverity, ratingLbReadiness),
    metrics: {
      p95LatencyMs: round(ratingLbP95LatencyMs, 1),
      failureRatePct: round(ratingLbFailureRatePct, 3),
      requestRateRps: round(ratingLbRequestRateRps, 2),
      lbRequestRateRps: round(rateSnapshot.ratingLbRequestRate, 2),
      lbFailureRatePct: round(rateSnapshot.ratingLbFailureRatePct, 3)
    }
  });

  for (const target of ratingTargets) {
    const requestRateRps = rateSnapshot.ratingTargetRequestRates[target.id] || 0;
    const failureRatePct = rateSnapshot.ratingTargetFailureRates[target.id] || 0;

    const baseSeverity = failureSeverity(failureRatePct, requestRateRps);
    const severity = target.healthy === false ? 'critical' : baseSeverity;

    nodes.push({
      id: target.id,
      type: 'service',
      label: target.id,
      severity,
      status: toNodeStatus(severity, target.healthy),
      metrics: {
        requestRateRps: round(requestRateRps, 2),
        failureRatePct: round(failureRatePct, 3),
        totalRequests: round(target.totalRequests, 0),
        totalErrors: round(target.totalErrors, 0),
        healthy: target.healthy,
        lastStatusCode: target.lastStatusCode || null,
        lastError: target.lastError || null,
        targetUrl: target.url || null
      }
    });
  }

  const redisReady = getDependencyState(readySnapshot, 'redis');
  const totalQueueDepth = (queueDepthByNode['enrichment-queue'] || 0) + (queueDepthByNode['critical-queue'] || 0);

  for (const queueNode of QUEUE_NODES) {
    const queueMetrics = queueMetricsByNode[queueNode.nodeId];

    const baseSeverity = maxSeverity([
      queueDepthSeverity(queueMetrics.depth),
      queueLagSeverity(queueMetrics.lagEstimateSec, queueMetrics.depth),
      rateSeverity(queueMetrics.failureRatePct)
    ]);

    const severity = redisReady === false ? 'critical' : baseSeverity;

    nodes.push({
      id: queueNode.nodeId,
      type: 'queue',
      label: queueNode.nodeId,
      severity,
      status: toNodeStatus(severity, redisReady),
      metrics: {
        depth: round(queueMetrics.depth, 0),
        completionRate: round(queueMetrics.completionRate, 2),
        failureRatePct: round(queueMetrics.failureRatePct, 3),
        lagEstimateSec: round(queueMetrics.lagEstimateSec, 2)
      }
    });
  }

  const enrichmentQueueSeverity = nodes.find((node) => node.id === 'enrichment-queue')?.severity || 'unknown';
  const criticalQueueSeverity = nodes.find((node) => node.id === 'critical-queue')?.severity || 'unknown';

  const enrichmentWorkerSeverity = redisReady === false ? 'critical' : enrichmentQueueSeverity;
  const criticalWorkerSeverity = redisReady === false ? 'critical' : criticalQueueSeverity;

  nodes.push({
    id: 'enrichment-worker',
    type: 'worker',
    label: 'enrichment-worker',
    severity: enrichmentWorkerSeverity,
    status: toNodeStatus(enrichmentWorkerSeverity, redisReady),
    metrics: {
      queue: 'enrichment-jobs',
      completionRate: round(queueMetricsByNode['enrichment-queue'].completionRate, 2),
      failureRatePct: round(queueMetricsByNode['enrichment-queue'].failureRatePct, 3),
      lagEstimateSec: round(queueMetricsByNode['enrichment-queue'].lagEstimateSec, 2)
    }
  });

  nodes.push({
    id: 'critical-worker',
    type: 'worker',
    label: 'critical-worker',
    severity: criticalWorkerSeverity,
    status: toNodeStatus(criticalWorkerSeverity, redisReady),
    metrics: {
      queue: 'critical-jobs',
      completionRate: round(queueMetricsByNode['critical-queue'].completionRate, 2),
      failureRatePct: round(queueMetricsByNode['critical-queue'].failureRatePct, 3),
      lagEstimateSec: round(queueMetricsByNode['critical-queue'].lagEstimateSec, 2)
    }
  });

  const redisSeverity = redisReady === false ? 'critical' : redisReady === true ? 'ok' : 'unknown';

  nodes.push({
    id: 'redis',
    type: 'infra',
    label: 'redis',
    severity: redisSeverity,
    status: toNodeStatus(redisSeverity, redisReady),
    metrics: {
      ready: redisReady,
      totalQueueDepth: round(totalQueueDepth, 0),
      readModelHits: round(readModelHits, 0),
      readModelMisses: round(readModelMisses, 0),
      readModelHitPct: round(readModelHitPct, 2),
      catalogCacheHits: round(catalogCacheHits, 0),
      catalogCacheMisses: round(catalogCacheMisses, 0),
      catalogCacheHitPct: round(catalogHitPct, 2)
    }
  });

  const nodesById = Object.fromEntries(nodes.map((node) => [node.id, node]));

  const edges = topologyEdges.map((edge) => {
    let metricsPayload = {};

    if (edge.kind === 'ingress') {
      metricsPayload = {
        totalIngressRps: round(ingressTotalRps, 2),
        requestTypeRps: Object.fromEntries(
          INGRESS_METRIC_KEYS.map((entry) => [
            entry.key,
            round(rateSnapshot.ingressRequestRates[entry.key] || 0, 2)
          ])
        )
      };
    }

    if (edge.kind === 'service') {
      const serviceNode = nodesById[edge.to];
      metricsPayload = {
        p95LatencyMs: serviceNode?.metrics?.p95LatencyMs,
        failureRatePct: serviceNode?.metrics?.failureRatePct,
        requestRateRps: serviceNode?.metrics?.requestRateRps
      };
    }

    if (edge.kind === 'queue') {
      const queueNode = nodesById[edge.to];
      metricsPayload = {
        depth: queueNode?.metrics?.depth,
        lagEstimateSec: queueNode?.metrics?.lagEstimateSec,
        completionRate: queueNode?.metrics?.completionRate
      };
    }

    if (edge.kind === 'worker') {
      const queueNode = nodesById[edge.from];
      metricsPayload = {
        queueDepth: queueNode?.metrics?.depth,
        completionRate: queueNode?.metrics?.completionRate,
        failureRatePct: queueNode?.metrics?.failureRatePct
      };
    }

    if (edge.kind === 'lb-target') {
      const targetNode = nodesById[edge.to];
      metricsPayload = {
        requestRateRps: targetNode?.metrics?.requestRateRps,
        failureRatePct: targetNode?.metrics?.failureRatePct,
        healthy: targetNode?.metrics?.healthy
      };
    }

    if (edge.kind === 'redis') {
      metricsPayload = {
        ready: redisReady,
        totalQueueDepth: nodesById.redis?.metrics?.totalQueueDepth
      };
    }

    if (edge.kind === 'cache') {
      metricsPayload = {
        readModelHitPct: nodesById.redis?.metrics?.readModelHitPct,
        catalogCacheHitPct: nodesById.redis?.metrics?.catalogCacheHitPct
      };
    }

    return {
      id: edge.id,
      from: edge.from,
      to: edge.to,
      severity: maxSeverity([
        nodesById[edge.from]?.severity || 'unknown',
        nodesById[edge.to]?.severity || 'unknown'
      ]),
      metrics: metricsPayload
    };
  });

  return {
    timestamp: new Date().toISOString(),
    ingress: {
      totalRps: round(ingressTotalRps, 2),
      byType: Object.fromEntries(
        INGRESS_METRIC_KEYS.map((entry) => [entry.key, round(rateSnapshot.ingressRequestRates[entry.key] || 0, 2)])
      )
    },
    nodes,
    edges
  };
}

async function buildSliResponse(requestId) {
  const metricsText = await fetchMetricsSnapshot(requestId);
  const metrics = parsePrometheusText(metricsText);

  const p95LatencySeconds = histogramQuantile(metrics, 'experience_http_request_duration_seconds', 0.95);
  const totalRequests = sumMetric(metrics, 'experience_http_request_duration_seconds_count');
  const totalErrors = sumMetric(metrics, 'experience_http_request_errors_total');

  const cumulativeErrorRate = totalRequests > 0 ? (totalErrors / totalRequests) * 100 : 0;

  const enrichmentQueueDepth = sumMetric(metrics, 'experience_queue_depth', { queue: 'enrichment-jobs' });
  const criticalQueueDepth = sumMetric(metrics, 'experience_queue_depth', { queue: 'critical-jobs' });

  const queueDepth = enrichmentQueueDepth + criticalQueueDepth;

  const upstreamTotal = sumMetric(metrics, 'experience_upstream_requests_total');
  const upstreamFailures = sumMetric(metrics, 'experience_upstream_failures_total');
  const cumulativeUpstreamFailureRate = upstreamTotal > 0 ? (upstreamFailures / upstreamTotal) * 100 : 0;

  const queueCompleted =
    sumMetric(metrics, 'experience_queue_jobs_completed_total', { queue: 'enrichment-jobs' }) +
    sumMetric(metrics, 'experience_queue_jobs_completed_total', { queue: 'critical-jobs' });

  const rates = computeRates({
    totalRequests,
    totalErrors,
    upstreamTotal,
    upstreamFailures,
    queueCompleted
  });

  const queueLagEstimateSeconds =
    rates.queueCompletionRatePerSecond > 0
      ? queueDepth / rates.queueCompletionRatePerSecond
      : queueDepth > 0
      ? Infinity
      : 0;

  const liveErrorRate = rates.errorRateDeltaPct || cumulativeErrorRate;
  const liveUpstreamFailureRate = rates.upstreamFailureRateDeltaPct || cumulativeUpstreamFailureRate;

  const sli = {
    timestamp: new Date().toISOString(),
    p95LatencyMs: p95LatencySeconds * 1000,
    errorRatePct: liveErrorRate,
    queueDepth,
    queueLagEstimateSeconds,
    upstreamFailureRatePct: liveUpstreamFailureRate,
    requestRate: rates.requestRate,
    queueCompletionRatePerSecond: rates.queueCompletionRatePerSecond
  };

  const slo = {
    targets: SLO_TARGETS,
    status: {
      p95Latency: sli.p95LatencyMs < SLO_TARGETS.p95LatencyMs,
      errorRate: sli.errorRatePct < SLO_TARGETS.errorRatePct,
      queueDepth: sli.queueDepth < SLO_TARGETS.queueDepth
    }
  };

  return { sli, slo };
}

async function buildHybridResponse(requestId) {
  const metricsText = await fetchMetricsSnapshot(requestId);
  const metrics = parsePrometheusText(metricsText);

  const readModelHits = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'hit' });
  const readModelMisses = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'miss' });
  const readModelStale = sumMetric(metrics, 'experience_read_model_outcomes_total', { outcome: 'stale' });
  const readModelLiveFallback = sumMetric(metrics, 'experience_read_model_outcomes_total', {
    outcome: 'live_fallback'
  });
  const readModelRefreshEnqueued = sumMetric(metrics, 'experience_read_model_outcomes_total', {
    outcome: 'refresh_enqueued'
  });

  const catalogCacheHits = sumMetric(metrics, 'experience_catalog_cache_outcomes_total', {
    outcome: 'hit'
  });
  const catalogCacheMisses = sumMetric(metrics, 'experience_catalog_cache_outcomes_total', {
    outcome: 'miss'
  });

  const dealsListRequests = sumMetric(metrics, 'experience_http_request_duration_seconds_count', {
    method: 'GET',
    route: '/api/deals'
  });

  const rates = computeHybridRates({
    readModelHits,
    readModelMisses,
    readModelStale,
    readModelLiveFallback,
    readModelRefreshEnqueued,
    catalogCacheHits,
    catalogCacheMisses,
    dealsListRequests
  });

  const totalReadModelLookups = readModelHits + readModelMisses + readModelStale;
  const readModelFreshHitPct = totalReadModelLookups > 0 ? (readModelHits / totalReadModelLookups) * 100 : 0;
  const readModelCoveragePct =
    totalReadModelLookups > 0 ? ((readModelHits + readModelStale) / totalReadModelLookups) * 100 : 0;

  const catalogTotal = catalogCacheHits + catalogCacheMisses;
  const catalogCacheHitPct = catalogTotal > 0 ? (catalogCacheHits / catalogTotal) * 100 : 0;

  const avgLiveFallbackPerListRequest =
    dealsListRequests > 0 ? readModelLiveFallback / dealsListRequests : 0;
  const liveAvgFromRates =
    rates.dealsListRps > 0 ? rates.liveFallbackRps / rates.dealsListRps : avgLiveFallbackPerListRequest;

  const classicFanoutPerRequest = 1 + HYBRID_ASSUMED_LIMIT * HYBRID_ASSUMED_ENRICHMENTS_PER_DEAL;
  const hybridFanoutPerRequestEstimate =
    1 + Math.min(Math.max(liveAvgFromRates, 0), HYBRID_ASSUMED_LIMIT) * HYBRID_ASSUMED_ENRICHMENTS_PER_DEAL;
  const fanoutReductionPct =
    classicFanoutPerRequest > 0
      ? ((classicFanoutPerRequest - hybridFanoutPerRequestEstimate) / classicFanoutPerRequest) * 100
      : 0;

  return {
    timestamp: new Date().toISOString(),
    architecture: {
      mode: 'hybrid',
      summary:
        'List traffic uses a Redis materialized read model, while detail and purchase paths still use live service composition.'
    },
    readModel: {
      totals: {
        hits: round(readModelHits, 0),
        misses: round(readModelMisses, 0),
        stale: round(readModelStale, 0),
        liveFallback: round(readModelLiveFallback, 0),
        refreshEnqueued: round(readModelRefreshEnqueued, 0)
      },
      rates: {
        hitRps: round(rates.readModelHitRps, 2),
        missRps: round(rates.readModelMissRps, 2),
        staleRps: round(rates.readModelStaleRps, 2),
        liveFallbackRps: round(rates.liveFallbackRps, 2),
        refreshEnqueueRps: round(rates.refreshEnqueueRps, 2)
      },
      freshHitPct: round(readModelFreshHitPct, 2),
      coveragePct: round(readModelCoveragePct, 2)
    },
    catalogCache: {
      totals: {
        hits: round(catalogCacheHits, 0),
        misses: round(catalogCacheMisses, 0)
      },
      rates: {
        hitRps: round(rates.catalogHitRps, 2),
        missRps: round(rates.catalogMissRps, 2)
      },
      hitPct: round(catalogCacheHitPct, 2)
    },
    listTraffic: {
      dealsListRps: round(rates.dealsListRps, 2),
      avgLiveFallbackPerListRequest: round(avgLiveFallbackPerListRequest, 3)
    },
    fanout: {
      classicPerRequest: round(classicFanoutPerRequest, 2),
      hybridPerRequestEstimate: round(hybridFanoutPerRequestEstimate, 2),
      reductionPct: round(fanoutReductionPct, 2)
    }
  };
}

const scenarioStates = new Map();
const SCENARIO_IDS = ['normal', 'spike', 'chaotic', 'high-throughput', 'enqueue-10k', 'hash-bad', 'hash-good'];

async function scenarioRequest({
  path: requestPath,
  method = 'GET',
  body,
  timeoutMs = 1600,
  scenarioId,
  context = 'tick'
}) {
  const correlationId = `dash-${scenarioId}-${context}-${Date.now()}-${Math.floor(Math.random() * 10_000)}`;
  const response = await fetch(`${experienceApiUrl}${requestPath}`, {
    method,
    headers: {
      'content-type': 'application/json',
      'x-correlation-id': correlationId
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeoutMs)
  });

  if (response.ok || response.status === 202 || response.status === 409 || response.status === 429) {
    return {
      ok: true,
      statusCode: response.status
    };
  }

  throw new Error(`HTTP ${response.status} for ${method} ${requestPath}`);
}

const scenarioRunners = {
  async normal(state) {
    const tick = state.tickCount + 1;
    const numericDealId = ((tick % 220) + 1).toString().padStart(5, '0');
    const dealId = `d-${numericDealId}`;

    const tasks = [
      scenarioRequest({
        scenarioId: state.id,
        context: 'deals-list',
        method: 'GET',
        path: '/api/deals?city=valencia&limit=6',
        timeoutMs: 1600
      }),
      scenarioRequest({
        scenarioId: state.id,
        context: 'deal-detail',
        method: 'GET',
        path: `/api/deals/${dealId}`,
        timeoutMs: 1500
      })
    ];

    if (tick % 4 === 0) {
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: 'purchase',
          method: 'POST',
          path: '/api/purchase',
          timeoutMs: 2000,
          body: {
            dealId,
            userId: `normal-user-${tick}`,
            quantity: 1
          }
        })
      );
    }

    const settled = await Promise.allSettled(tasks);
    const successCount = settled.filter((entry) => entry.status === 'fulfilled').length;
    const failureCount = settled.length - successCount;

    return {
      successCount,
      failureCount,
      details: 'Baseline traffic loop generating steady reads and periodic purchases.'
    };
  },

  async spike(state) {
    const burstSize = 14;
    const requests = Array.from({ length: burstSize }).map((_, index) =>
      scenarioRequest({
        scenarioId: state.id,
        context: `burst-${index}`,
        method: 'GET',
        path: '/api/deals?city=valencia&limit=20',
        timeoutMs: 1800
      })
    );

    const settled = await Promise.allSettled(requests);
    const successCount = settled.filter((entry) => entry.status === 'fulfilled').length;
    const failureCount = settled.length - successCount;

    return {
      successCount,
      failureCount,
      details: `Burst fan-out sent ${burstSize} parallel list requests.`
    };
  },

  async chaotic(state) {
    const tick = state.tickCount + 1;
    const listBurst = 8 + Math.floor(Math.random() * 10);
    const detailBurst = 4 + Math.floor(Math.random() * 6);
    const tasks = [];

    for (let index = 0; index < listBurst; index += 1) {
      const limit = Math.random() < 0.5 ? 20 : 6;
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `list-${tick}-${index}`,
          method: 'GET',
          path: `/api/deals?city=valencia&limit=${limit}`,
          timeoutMs: 2200
        })
      );
    }

    for (let index = 0; index < detailBurst; index += 1) {
      const numericDealId = ((Math.floor(Math.random() * 220) + 1) % 221).toString().padStart(5, '0');
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `detail-${tick}-${index}`,
          method: 'GET',
          path: `/api/deals/d-${numericDealId}`,
          timeoutMs: 2600
        })
      );
    }

    if (Math.random() < 0.65) {
      const numericDealId = ((Math.floor(Math.random() * 220) + 1) % 221).toString().padStart(5, '0');
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `purchase-${tick}`,
          method: 'POST',
          path: '/api/purchase',
          timeoutMs: 2500,
          body: {
            dealId: `d-${numericDealId}`,
            userId: `chaos-user-${tick}`,
            quantity: 1
          }
        })
      );
    }

    if (Math.random() < 0.4) {
      const enqueueCount = 300 + Math.floor(Math.random() * 1700);
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `enqueue-${tick}`,
          method: 'POST',
          path: '/api/enrichment-jobs',
          timeoutMs: 2800,
          body: {
            count: enqueueCount,
            city: 'valencia'
          }
        })
      );
    }

    const hashRoll = Math.random();
    if (hashRoll < 0.18) {
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `hash-bad-${tick}`,
          method: 'POST',
          path: '/api/heavy-hash/bad',
          timeoutMs: 8000,
          body: {
            rounds: 2,
            iterations: 180_000
          }
        })
      );
    } else if (hashRoll < 0.45) {
      tasks.push(
        scenarioRequest({
          scenarioId: state.id,
          context: `hash-good-${tick}`,
          method: 'POST',
          path: '/api/heavy-hash/good',
          timeoutMs: 8000,
          body: {
            rounds: 2,
            iterations: 180_000
          }
        })
      );
    }

    const settled = await Promise.allSettled(tasks);
    const successCount = settled.filter((entry) => entry.status === 'fulfilled').length;
    const failureCount = settled.length - successCount;

    return {
      successCount,
      failureCount,
      details: `Chaotic mix: list=${listBurst}, detail=${detailBurst}, tasks=${tasks.length}.`
    };
  },

  async 'high-throughput'(state) {
    const waveConcurrency = 200;
    const requests = Array.from({ length: waveConcurrency }).map((_, index) =>
      scenarioRequest({
        scenarioId: state.id,
        context: `wave-${state.tickCount + 1}-${index}`,
        method: 'GET',
        path: '/api/deals?city=valencia&limit=20',
        timeoutMs: 2200
      })
    );

    const settled = await Promise.allSettled(requests);
    const successCount = settled.filter((entry) => entry.status === 'fulfilled').length;
    const failureCount = settled.length - successCount;

    return {
      successCount,
      failureCount,
      details:
        'High-throughput wave executed with 200 concurrent /api/deals requests (targeting ~2.5k-3k ingress rps on local Docker).'
    };
  },

  async 'enqueue-10k'(state) {
    const result = await scenarioRequest({
      scenarioId: state.id,
      context: 'enqueue-batch',
      method: 'POST',
      path: '/api/enrichment-jobs',
      timeoutMs: 2500,
      body: {
        count: 10_000,
        city: 'valencia'
      }
    });

    return {
      successCount: 1,
      failureCount: 0,
      details: `Enqueue batch submitted (status ${result.statusCode}).`
    };
  },

  async 'hash-bad'(state) {
    const result = await scenarioRequest({
      scenarioId: state.id,
      context: 'hash-main-thread',
      method: 'POST',
      path: '/api/heavy-hash/bad',
      timeoutMs: 7000,
      body: {
        rounds: 3,
        iterations: 230_000
      }
    });

    return {
      successCount: 1,
      failureCount: 0,
      details: `Main-thread hash request completed (status ${result.statusCode}).`
    };
  },

  async 'hash-good'(state) {
    const result = await scenarioRequest({
      scenarioId: state.id,
      context: 'hash-worker-thread',
      method: 'POST',
      path: '/api/heavy-hash/good',
      timeoutMs: 7000,
      body: {
        rounds: 3,
        iterations: 230_000
      }
    });

    return {
      successCount: 1,
      failureCount: 0,
      details: `Worker-thread hash request completed (status ${result.statusCode}).`
    };
  }
};

const scenarioDefinitions = {
  normal: {
    id: 'normal',
    label: 'Normal traffic',
    intervalMs: 700,
    defaultActive: true,
    summary:
      'Steady user-like baseline traffic across deal browsing and occasional purchases to keep the system warm.',
    explanation:
      'This scenario continuously generates realistic marketplace read traffic plus periodic purchases. It drives ingress RPS, upstream fan-out, and queue activity at a stable baseline.'
  },
  spike: {
    id: 'spike',
    label: 'Spike traffic',
    intervalMs: 280,
    defaultActive: false,
    summary:
      'Sudden high-concurrency read bursts to stress API latency, retries, and upstream saturation behavior.',
    explanation:
      'Spike sends parallel bursts of /api/deals requests. You should see ingress RPS jump, p95 latency rise, and service severities degrade when timeouts/retries increase.'
  },
  chaotic: {
    id: 'chaotic',
    label: 'Chaotic scenario',
    intervalMs: 1700,
    defaultActive: false,
    summary:
      'Mixed unpredictable traffic and background stress to simulate cascading failures and noisy production bursts.',
    explanation:
      'Each cycle mixes list/detail fan-out, purchases, random queue bursts, and occasional hash workloads. Expect volatile latency/error swings and changing pressure across API, queues, and workers.'
  },
  'high-throughput': {
    id: 'high-throughput',
    label: 'High throughput (max load)',
    intervalMs: 10,
    defaultActive: false,
    summary:
      'Sustained high-concurrency list traffic to approximate benchmark-level ingress and visualize system limits.',
    explanation:
      'This scenario continuously fires 200 concurrent /api/deals requests per wave. Use it to observe near-max local throughput, queueing effects, and where latency starts to bend.'
  },
  'enqueue-10k': {
    id: 'enqueue-10k',
    label: 'Enqueue 10k jobs',
    intervalMs: 12_000,
    defaultActive: false,
    summary: 'Repeatedly pushes large enrichment batches to trigger queue backpressure and lag growth.',
    explanation:
      'Each interval submits 10k enrichment jobs. Queue depth and lag climb quickly, workers become saturated, and admission control can return 429 when thresholds are exceeded.'
  },
  'hash-bad': {
    id: 'hash-bad',
    label: 'Trigger blocking hash',
    intervalMs: 5_000,
    defaultActive: false,
    summary: 'Forces CPU work onto the event loop, demonstrating blocking-induced tail latency and timeout cascades.',
    explanation:
      'The bad hash runs PBKDF2 on the main thread. This blocks the event loop, so unrelated requests wait, upstream timeouts increase, and error rate may spike.'
  },
  'hash-good': {
    id: 'hash-good',
    label: 'Trigger worker hash',
    intervalMs: 5_000,
    defaultActive: false,
    summary:
      'Runs the same CPU workload via worker threads, preserving API responsiveness while doing heavy compute.',
    explanation:
      'The good hash offloads CPU work to worker threads. Compare this with hash-bad: similar compute happens, but event-loop latency impact is lower.'
  }
};

function createScenarioState(definition) {
  return {
    id: definition.id,
    label: definition.label,
    summary: definition.summary,
    explanation: definition.explanation,
    intervalMs: definition.intervalMs,
    defaultActive: Boolean(definition.defaultActive),
    active: false,
    timer: null,
    inProgress: false,
    tickCount: 0,
    startedAt: null,
    stoppedAt: null,
    lastRunAt: null,
    lastError: null,
    lastOutcome: null
  };
}

function scenarioActiveNarrative(activeStates) {
  if (activeStates.length === 0) {
    return 'No active scenario. Ingress traffic reflects only manual requests.';
  }

  if (activeStates.length === 1 && activeStates[0].id === 'normal') {
    return 'Normal baseline traffic is active: steady marketplace-like reads and periodic purchases.';
  }

  const labels = activeStates.map((state) => state.label).join(', ');
  return `Active scenarios: ${labels}. Expect compounded impact across latency, errors, queue pressure, and load balancing distribution.`;
}

function serializeScenarioState(state) {
  return {
    id: state.id,
    label: state.label,
    summary: state.summary,
    explanation: state.explanation,
    intervalMs: state.intervalMs,
    defaultActive: state.defaultActive,
    active: state.active,
    startedAt: state.startedAt,
    stoppedAt: state.stoppedAt,
    lastRunAt: state.lastRunAt,
    lastError: state.lastError,
    lastOutcome: state.lastOutcome
  };
}

function scenarioSnapshot() {
  const scenarios = SCENARIO_IDS.map((id) => serializeScenarioState(scenarioStates.get(id)));
  const activeScenarios = scenarios.filter((scenario) => scenario.active);

  return {
    timestamp: new Date().toISOString(),
    defaultScenarioId: 'normal',
    activeScenarioIds: activeScenarios.map((scenario) => scenario.id),
    activeNarrative: scenarioActiveNarrative(activeScenarios),
    scenarios
  };
}

function stopScenario(id, reason = 'manual') {
  const state = scenarioStates.get(id);
  if (!state || !state.active) {
    return state;
  }

  state.active = false;
  state.stoppedAt = new Date().toISOString();

  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }

  logger.info({ scenario: id, reason }, 'Scenario deactivated');
  return state;
}

function runScenarioTick(id) {
  const state = scenarioStates.get(id);
  if (!state || !state.active || shuttingDown || state.inProgress) {
    return;
  }

  const runner = scenarioRunners[id];
  if (!runner) {
    return;
  }

  state.inProgress = true;

  runner(state)
    .then((outcome) => {
      state.tickCount += 1;
      state.lastRunAt = new Date().toISOString();
      state.lastError = null;
      state.lastOutcome = outcome || null;
    })
    .catch((error) => {
      state.tickCount += 1;
      state.lastRunAt = new Date().toISOString();
      state.lastError = error.message;
      state.lastOutcome = null;
      logger.warn({ scenario: id, err: error }, 'Scenario tick failed');
    })
    .finally(() => {
      state.inProgress = false;
    });
}

function startScenario(id, reason = 'manual') {
  const state = scenarioStates.get(id);
  if (!state || state.active) {
    return state;
  }

  state.active = true;
  state.startedAt = new Date().toISOString();
  state.stoppedAt = null;
  state.lastError = null;

  state.timer = setInterval(() => {
    runScenarioTick(id);
  }, state.intervalMs);

  state.timer.unref();
  runScenarioTick(id);

  logger.info({ scenario: id, reason }, 'Scenario activated');
  return state;
}

function initializeScenarios() {
  for (const id of SCENARIO_IDS) {
    const definition = scenarioDefinitions[id];
    if (!definition) {
      throw new Error(`Missing scenario definition for ${id}`);
    }

    scenarioStates.set(id, createScenarioState(definition));
  }

  for (const id of SCENARIO_IDS) {
    if (scenarioDefinitions[id]?.defaultActive) {
      startScenario(id, 'startup-default');
    }
  }
}

function stopAllScenarios() {
  for (const id of SCENARIO_IDS) {
    stopScenario(id, 'shutdown');
  }
}

initializeScenarios();

app.get('/chart.js', (_req, res) => {
  res.sendFile(chartJsFile);
});

app.get('/healthz', (req, res) => {
  res.json({
    status: 'ok',
    service: serviceName,
    requestId: req.id
  });
});

app.get('/api/sli', async (req, res) => {
  try {
    const payload = await buildSliResponse(req.id);
    res.json(payload);
  } catch (error) {
    req.log.error({ err: error }, 'Failed to compute SLI payload');
    res.status(503).json({
      error: 'metrics scrape failed',
      requestId: req.id
    });
  }
});

app.get('/api/hybrid', async (req, res) => {
  try {
    const payload = await buildHybridResponse(req.id);
    res.json(payload);
  } catch (error) {
    req.log.error({ err: error }, 'Failed to compute hybrid dashboard payload');
    res.status(503).json({
      error: 'hybrid metrics unavailable',
      requestId: req.id
    });
  }
});

app.get('/api/topology', async (req, res) => {
  try {
    const payload = await buildTopologyResponse(req.id);
    res.json(payload);
  } catch (error) {
    req.log.error({ err: error }, 'Failed to compute topology payload');
    res.status(503).json({
      error: 'topology unavailable',
      requestId: req.id
    });
  }
});

app.get('/api/scenarios', (req, res) => {
  res.json({
    ...scenarioSnapshot(),
    requestId: req.id
  });
});

app.post('/api/scenario/:name', (req, res) => {
  const scenario = req.params.name;
  const state = scenarioStates.get(scenario);

  if (!state) {
    res.status(404).json({
      error: `Unknown scenario: ${scenario}`,
      requestId: req.id
    });
    return;
  }

  const desiredActive =
    typeof req.body?.active === 'boolean' ? req.body.active : !state.active;

  if (desiredActive) {
    startScenario(scenario, `api-request:${req.id}`);
  } else {
    stopScenario(scenario, `api-request:${req.id}`);
  }

  res.status(202).json({
    accepted: true,
    scenario,
    active: scenarioStates.get(scenario).active,
    ...scenarioSnapshot(),
    requestId: req.id
  });
});

app.use(express.static(path.join(__dirname, 'public')));

const server = app.listen(port, () => {
  logger.info({ port, metricsUrl, ratingLbStatsUrl }, 'slo-dashboard listening');
});

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  logger.info({ signal }, 'Starting slo-dashboard shutdown');

  stopAllScenarios();

  await new Promise((resolve) => {
    server.close(() => resolve());
  });

  const deadline = Date.now() + 10_000;
  while (inFlight > 0 && Date.now() < deadline) {
    await sleep(50);
  }

  await shutdownTracing(sdk, logger);
  logger.info({ inFlight }, 'slo-dashboard stopped');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM');
});
