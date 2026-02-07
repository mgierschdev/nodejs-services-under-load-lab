const { fetch } = require('undici');
const { sleep, jitter, nsToSeconds } = require('./utils');

function isTimeoutError(error) {
  if (!error) {
    return false;
  }

  const timeoutCodes = new Set([
    'ABORT_ERR',
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_BODY_TIMEOUT'
  ]);

  return (
    error.name === 'TimeoutError' ||
    timeoutCodes.has(error.code) ||
    /timed?\s*out/i.test(error.message || '')
  );
}

async function fetchJsonWithResilience({
  url,
  method = 'GET',
  body,
  headers = {},
  timeoutMs = 800,
  retries = 1,
  requestId,
  serviceName,
  logger,
  metrics
}) {
  const upperMethod = method.toUpperCase();
  const canRetry = upperMethod === 'GET';
  const maxAttempts = canRetry ? retries + 1 : 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const start = process.hrtime.bigint();

    try {
      const response = await fetch(url, {
        method: upperMethod,
        headers: {
          'content-type': 'application/json',
          'x-correlation-id': requestId,
          ...headers
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(timeoutMs)
      });

      const durationSeconds = nsToSeconds(Number(process.hrtime.bigint() - start));

      if (metrics?.onAttempt) {
        metrics.onAttempt({
          serviceName,
          durationSeconds,
          status: response.ok ? 'ok' : 'error'
        });
      }

      if (!response.ok) {
        const errorBody = await response.text();
        const error = new Error(
          `HTTP ${response.status} from ${serviceName} (${url}): ${errorBody.slice(0, 200)}`
        );
        error.statusCode = response.status;
        throw error;
      }

      const contentType = response.headers.get('content-type') || '';
      if (contentType.includes('application/json')) {
        return response.json();
      }

      const text = await response.text();
      return { text };
    } catch (error) {
      const durationSeconds = nsToSeconds(Number(process.hrtime.bigint() - start));
      const timeout = isTimeoutError(error);

      if (metrics?.onAttempt) {
        metrics.onAttempt({
          serviceName,
          durationSeconds,
          status: timeout ? 'timeout' : 'error'
        });
      }

      const shouldRetry = canRetry && attempt < maxAttempts;

      if (!shouldRetry) {
        if (logger) {
          logger.warn(
            {
              err: error,
              attempt,
              serviceName,
              timeout,
              url
            },
            'Upstream request failed'
          );
        }

        throw error;
      }

      if (logger) {
        logger.warn(
          {
            err: error,
            attempt,
            serviceName,
            timeout,
            url
          },
          'Retrying upstream GET request'
        );
      }

      await sleep(jitter(30, 180));
    }
  }

  throw new Error('Unexpected resilience loop exit');
}

module.exports = {
  fetchJsonWithResilience,
  isTimeoutError
};
