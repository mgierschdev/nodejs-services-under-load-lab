const { createDomainService } = require('../../shared/domain-runtime');
const { getDeal } = require('../../shared/data');

const instanceId = process.env.INSTANCE_ID || 'rating-service';
const baseServiceName = 'rating-service';
const serviceName = instanceId === baseServiceName ? baseServiceName : `${baseServiceName}-${instanceId}`;

function buildRating(dealId) {
  const numeric = Number(dealId.replace(/[^0-9]/g, '')) || 1;
  const rating = Number((3.2 + (numeric % 18) * 0.1).toFixed(1));

  return {
    dealId,
    rating: Math.min(5, rating),
    reviewCount: 20 + (numeric % 600),
    recommendationRate: Number((72 + (numeric % 27)).toFixed(1)),
    instanceId
  };
}

createDomainService({
  serviceName,
  defaultPort: 3004,
  latency: { min: 40, max: 320 },
  failureRate: 0.02,
  registerRoutes: (app) => {
    app.get('/rating/:dealId', (req, res) => {
      const deal = getDeal(req.params.dealId);

      if (!deal) {
        res.status(404).json({
          error: 'Deal not found',
          requestId: req.id
        });
        return;
      }

      res.json({
        rating: buildRating(deal.id),
        requestId: req.id
      });
    });
  }
});
