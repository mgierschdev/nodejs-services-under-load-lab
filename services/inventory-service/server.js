const { createDomainService } = require('../../shared/domain-runtime');
const { getDeal } = require('../../shared/data');

function buildInventory(dealId) {
  const numeric = Number(dealId.replace(/[^0-9]/g, '')) || 1;
  const stock = (numeric * 13) % 120;

  return {
    dealId,
    available: stock > 5,
    stock,
    reserved: Math.floor(stock * 0.12),
    lowInventory: stock < 15
  };
}

createDomainService({
  serviceName: 'inventory-service',
  defaultPort: 3003,
  latency: { min: 20, max: 210 },
  failureRate: 0.01,
  registerRoutes: (app) => {
    app.get('/inventory/:dealId', (req, res) => {
      const deal = getDeal(req.params.dealId);

      if (!deal) {
        res.status(404).json({
          error: 'Deal not found',
          requestId: req.id
        });
        return;
      }

      res.json({
        inventory: buildInventory(deal.id),
        requestId: req.id
      });
    });
  }
});
