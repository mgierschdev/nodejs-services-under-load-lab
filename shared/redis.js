const IORedis = require('ioredis');

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

function createRedisClient(connectionName = 'default') {
  return new IORedis(redisUrl, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    connectionName
  });
}

module.exports = {
  redisUrl,
  createRedisClient
};
