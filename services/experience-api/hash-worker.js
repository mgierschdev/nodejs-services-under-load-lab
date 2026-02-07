const { parentPort } = require('node:worker_threads');
const { pbkdf2Sync } = require('node:crypto');

function runHash({ payload, rounds, iterations }) {
  let digest = '';

  for (let i = 0; i < rounds; i += 1) {
    digest = pbkdf2Sync(payload, `salt-${i}`, iterations, 64, 'sha512').toString('hex');
  }

  return digest;
}

parentPort.on('message', (message) => {
  const startedAt = Date.now();

  try {
    const digest = runHash(message);
    parentPort.postMessage({
      digest,
      durationMs: Date.now() - startedAt
    });
  } catch (error) {
    parentPort.postMessage({
      error: error.message,
      durationMs: Date.now() - startedAt
    });
  }
});
