const cities = ['valencia', 'madrid', 'barcelona', 'lisbon', 'berlin', 'paris', 'rome'];
const categories = ['food', 'spa', 'activities', 'travel', 'fitness', 'beauty'];

const merchantNames = [
  'Sunset Bistro',
  'Urban Escape Spa',
  'Skyline Adventures',
  'Coastal Fitness Club',
  'Golden Spoon',
  'Zen Harbor Retreat',
  'Pulse Yoga Studio',
  'City Cycle Tours',
  'Moonlight Dining',
  'Luna Wellness'
];

const merchants = merchantNames.map((name, index) => {
  const city = cities[index % cities.length];
  const id = `m-${(index + 1).toString().padStart(3, '0')}`;

  return {
    id,
    name,
    city,
    verified: index % 3 !== 0,
    responseTimeHours: 2 + (index % 12),
    fulfillmentScore: Number((4.1 + (index % 9) * 0.08).toFixed(2))
  };
});

const deals = Array.from({ length: 220 }).map((_, index) => {
  const city = cities[index % cities.length];
  const merchant = merchants[index % merchants.length];
  const category = categories[index % categories.length];
  const basePrice = 15 + ((index * 7) % 120);

  return {
    id: `d-${(index + 1).toString().padStart(5, '0')}`,
    title: `${category.toUpperCase()} Deal #${index + 1}`,
    summary: `Premium ${category} offer in ${city}`,
    city,
    category,
    merchantId: merchant.id,
    currency: 'EUR',
    basePrice
  };
});

const dealById = new Map(deals.map((deal) => [deal.id, deal]));
const merchantById = new Map(merchants.map((merchant) => [merchant.id, merchant]));

function findDealsByCity(city, limit = 20) {
  const normalizedCity = (city || '').toLowerCase().trim();

  const rows = normalizedCity
    ? deals.filter((deal) => deal.city === normalizedCity)
    : deals;

  return rows.slice(0, limit);
}

function getDeal(id) {
  return dealById.get(id) || null;
}

function getMerchant(id) {
  return merchantById.get(id) || null;
}

module.exports = {
  deals,
  merchants,
  findDealsByCity,
  getDeal,
  getMerchant
};
