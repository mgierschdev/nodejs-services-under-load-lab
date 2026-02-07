const { createDomainService } = require('../../shared/domain-runtime');
const { findDealsByCity, getDeal } = require('../../shared/data');

createDomainService({
  serviceName: 'deal-service',
  defaultPort: 3001,
  latency: { min: 20, max: 180 },
  failureRate: 0.01,
  registerRoutes: (app) => {
    app.get('/deals', (req, res) => {
      const city = (req.query.city || '').toString().toLowerCase().trim();
      const limit = Math.min(Number(req.query.limit) || 20, 100);

      res.json({
        deals: findDealsByCity(city, limit),
        requestId: req.id
      });
    });

    app.get('/deals/:id', (req, res) => {
      const deal = getDeal(req.params.id);

      if (!deal) {
        res.status(404).json({
          error: 'Deal not found',
          requestId: req.id
        });
        return;
      }

      res.json({
        deal,
        requestId: req.id
      });
    });
  }
});
