const { createDomainService } = require('../../shared/domain-runtime');
const { getDeal } = require('../../shared/data');

function calculatePricing(dealId, basePrice) {
  const numeric = Number(dealId.replace(/[^0-9]/g, '')) || 1;
  const discountPct = 10 + (numeric % 55);
  const discounted = Number((basePrice * (1 - discountPct / 100)).toFixed(2));

  return {
    dealId,
    listPrice: Number(basePrice.toFixed(2)),
    discountPct,
    finalPrice: discounted,
    savings: Number((basePrice - discounted).toFixed(2)),
    currency: 'EUR'
  };
}

createDomainService({
  serviceName: 'price-service',
  defaultPort: 3002,
  latency: { min: 35, max: 250 },
  failureRate: 0.015,
  registerRoutes: (app) => {
    app.get('/price/:dealId', (req, res) => {
      const deal = getDeal(req.params.dealId);

      if (!deal) {
        res.status(404).json({
          error: 'Deal not found',
          requestId: req.id
        });
        return;
      }

      res.json({
        pricing: calculatePricing(deal.id, deal.basePrice),
        requestId: req.id
      });
    });
  }
});
