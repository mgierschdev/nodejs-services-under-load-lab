const { setTimeout: sleep } = require('node:timers/promises');

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function chance(probability) {
  return Math.random() < probability;
}

function jitter(minMs = 25, maxMs = 125) {
  return randomInt(minMs, maxMs);
}

function nsToSeconds(ns) {
  return ns / 1e9;
}

module.exports = {
  sleep,
  randomInt,
  chance,
  jitter,
  nsToSeconds
};
