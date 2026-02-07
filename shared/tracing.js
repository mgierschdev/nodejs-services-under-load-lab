const { diag, DiagConsoleLogger, DiagLogLevel } = require('@opentelemetry/api');
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { ConsoleSpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');

function startTracing(serviceName) {
  if (!serviceName) {
    throw new Error('serviceName is required for tracing startup');
  }

  process.env.OTEL_SERVICE_NAME = process.env.OTEL_SERVICE_NAME || serviceName;

  if (process.env.OTEL_DIAG === 'true') {
    diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);
  }

  const sdkConfig = {
    instrumentations: [getNodeAutoInstrumentations()]
  };

  if (process.env.OTEL_CONSOLE_EXPORTER === 'true') {
    sdkConfig.spanProcessor = new SimpleSpanProcessor(new ConsoleSpanExporter());
  }

  const sdk = new NodeSDK(sdkConfig);

  // Best effort startup: support both sync and async start() return styles.
  try {
    const maybePromise = sdk.start();

    if (maybePromise && typeof maybePromise.then === 'function') {
      maybePromise.catch((error) => {
        // eslint-disable-next-line no-console
        console.error('OpenTelemetry startup failed:', error.message);
      });
    }
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('OpenTelemetry startup failed:', error.message);
  }

  return sdk;
}

async function shutdownTracing(sdk, logger) {
  if (!sdk) {
    return;
  }

  try {
    await sdk.shutdown();
  } catch (error) {
    if (logger) {
      logger.warn({ err: error }, 'Tracing shutdown failed');
    }
  }
}

module.exports = {
  startTracing,
  shutdownTracing
};
