const { Queue } = require('bullmq');
const { createRedisClient } = require('./redis');

function createQueue(name, connectionName) {
  const connection = createRedisClient(connectionName || `queue-${name}`);
  const queue = new Queue(name, {
    connection,
    defaultJobOptions: {
      removeOnComplete: 5000,
      removeOnFail: 5000
    }
  });

  return { queue, connection };
}

async function getQueueDepth(queue) {
  const counts = await queue.getJobCounts(
    'waiting',
    'active',
    'delayed',
    'paused',
    'prioritized',
    'waiting-children'
  );

  return (
    (counts.waiting || 0) +
    (counts.active || 0) +
    (counts.delayed || 0) +
    (counts.paused || 0) +
    (counts.prioritized || 0) +
    (counts['waiting-children'] || 0)
  );
}

module.exports = {
  createQueue,
  getQueueDepth
};
