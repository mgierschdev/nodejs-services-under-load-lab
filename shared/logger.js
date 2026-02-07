const pino = require('pino');

function createLogger(serviceName) {
  return pino({
    level: process.env.LOG_LEVEL || 'info',
    base: {
      service: serviceName
    },
    timestamp: pino.stdTimeFunctions.isoTime
  });
}

module.exports = {
  createLogger
};
