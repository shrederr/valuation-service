const { Client } = require('pg');

async function run() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  await client.connect();

  // Check external_url patterns
  const urls = await client.query(`
    SELECT
      CASE
        WHEN external_url LIKE '%olx.ua%' THEN 'olx'
        WHEN external_url LIKE '%dom.ria%' THEN 'domRia'
        WHEN external_url LIKE '%realtor.ua%' THEN 'realtorUa'
        WHEN external_url LIKE '%realestate.lviv%' THEN 'realEstateLvivUa'
        WHEN external_url LIKE '%mls.%' THEN 'mlsUkraine'
        WHEN external_url IS NULL THEN 'NO_URL'
        ELSE 'OTHER'
      END as platform,
      COUNT(*) as cnt
    FROM unified_listings
    GROUP BY 1
    ORDER BY cnt DESC
  `);

  console.log('=== Определение платформы по external_url ===');
  urls.rows.forEach(r => console.log(r.platform + ': ' + r.cnt));

  // Show some OTHER examples
  const others = await client.query(`
    SELECT external_url FROM unified_listings
    WHERE external_url IS NOT NULL
      AND external_url NOT LIKE '%olx.ua%'
      AND external_url NOT LIKE '%dom.ria%'
      AND external_url NOT LIKE '%realtor.ua%'
      AND external_url NOT LIKE '%realestate.lviv%'
      AND external_url NOT LIKE '%mls.%'
    LIMIT 5
  `);

  if (others.rows.length > 0) {
    console.log('\n=== Примеры OTHER ===');
    others.rows.forEach(r => console.log(r.external_url));
  }

  await client.end();
}

run().catch(console.error);
