const { Client } = require('pg');

// Change to Railway connection when needed
const DB_CONFIG = {
  // Local
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'

  // Railway (uncomment to use):
  // connectionString: 'postgresql://postgres:wGsLFLgUVbkWMLfhZalccgPikVGilesU@mainline.proxy.rlwy.net:40041/railway',
  // ssl: { rejectUnauthorized: false }
};

async function run() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to database');

  // Update realty_platform based on external_url patterns
  console.log('\nUpdating realty_platform...');

  const result = await client.query(`
    UPDATE unified_listings
    SET realty_platform = CASE
      WHEN external_url LIKE '%olx.ua%' THEN 'olx'
      WHEN external_url LIKE '%dom.ria%' THEN 'domRia'
      WHEN external_url LIKE '%rieltor.ua%' OR external_url LIKE '%realtor.ua%' THEN 'realtorUa'
      WHEN external_url LIKE '%real-estate.lviv%' THEN 'realEstateLvivUa'
      WHEN external_url LIKE '%mls%ukraine%' OR external_url LIKE '%mlsukraine%' THEN 'mlsUkraine'
      ELSE NULL
    END
    WHERE realty_platform IS NULL
  `);

  console.log(`Updated ${result.rowCount} rows`);

  // Verify
  const check = await client.query(`
    SELECT realty_platform, COUNT(*) as cnt
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY cnt DESC
  `);

  console.log('\n=== После обновления ===');
  check.rows.forEach(r => console.log((r.realty_platform || 'NULL') + ': ' + r.cnt));

  await client.end();
}

run().catch(console.error);
