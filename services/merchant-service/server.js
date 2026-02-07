const { createDomainService } = require('../../shared/domain-runtime');
const { getMerchant } = require('../../shared/data');

createDomainService({
  serviceName: 'merchant-service',
  defaultPort: 3005,
  latency: { min: 30, max: 260 },
  failureRate: 0.015,
  registerRoutes: (app) => {
    app.get('/merchant/:merchantId', (req, res) => {
      const merchant = getMerchant(req.params.merchantId);

      if (!merchant) {
        res.status(404).json({
          error: 'Merchant not found',
          requestId: req.id
        });
        return;
      }

      res.json({
        merchant,
        requestId: req.id
      });
    });
  }
});
