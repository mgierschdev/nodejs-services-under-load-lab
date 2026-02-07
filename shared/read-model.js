const DEAL_CARD_PREFIX = 'experience:deal-card:v1';

function dealCardKey(dealId) {
  return `${DEAL_CARD_PREFIX}:${dealId}`;
}

function parseDealCard(rawPayload) {
  if (!rawPayload) {
    return null;
  }

  try {
    const parsed = JSON.parse(rawPayload);
    if (!parsed || typeof parsed !== 'object') {
      return null;
    }
    return parsed;
  } catch (_error) {
    return null;
  }
}

function buildDealCardSnapshot({
  dealId,
  city = null,
  merchantId = null,
  pricing = null,
  inventory = null,
  rating = null,
  merchant = null,
  source = 'unknown',
  warnings = []
}) {
  const updatedAtMs = Date.now();

  return {
    schema: 'deal-card-v1',
    dealId,
    city,
    merchantId,
    pricing,
    inventory,
    rating,
    merchant,
    source,
    warnings,
    updatedAtMs,
    updatedAt: new Date(updatedAtMs).toISOString()
  };
}

function snapshotUpdatedAtMs(snapshot) {
  if (!snapshot) {
    return 0;
  }

  if (Number.isFinite(Number(snapshot.updatedAtMs))) {
    return Number(snapshot.updatedAtMs);
  }

  if (snapshot.updatedAt) {
    const parsed = Date.parse(snapshot.updatedAt);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

function isDealCardFresh(snapshot, staleMs, nowMs = Date.now()) {
  const updatedAtMs = snapshotUpdatedAtMs(snapshot);
  if (!updatedAtMs || !Number.isFinite(staleMs) || staleMs <= 0) {
    return false;
  }

  return nowMs - updatedAtMs <= staleMs;
}

function mergeDealWithSnapshot(deal, snapshot) {
  return {
    ...deal,
    pricing: snapshot?.pricing || null,
    inventory: snapshot?.inventory || null,
    rating: snapshot?.rating || null,
    merchant: snapshot?.merchant || null
  };
}

module.exports = {
  DEAL_CARD_PREFIX,
  dealCardKey,
  parseDealCard,
  buildDealCardSnapshot,
  snapshotUpdatedAtMs,
  isDealCardFresh,
  mergeDealWithSnapshot
};
