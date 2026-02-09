const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  const queries = [
    { label: 'Total listings', sql: 'SELECT COUNT(*) as c FROM unified_listings' },
    { label: 'With geo_id', sql: 'SELECT COUNT(*) as c FROM unified_listings WHERE geo_id IS NOT NULL' },
    { label: 'With street_id', sql: 'SELECT COUNT(*) as c FROM unified_listings WHERE street_id IS NOT NULL' },
    { label: 'No geo (non-OLX)', sql: "SELECT COUNT(*) as c FROM unified_listings WHERE geo_id IS NULL AND realty_platform != 'olx'" },
    { label: 'No geo (all)', sql: 'SELECT COUNT(*) as c FROM unified_listings WHERE geo_id IS NULL' },
  ];

  for (const q of queries) {
    const res = await client.query(q.sql);
    console.log(`${q.label}: ${res.rows[0].c}`);
  }

  // Check by platform
  console.log('\n--- By platform (no geo_id) ---');
  const byPlatform = await client.query(`
    SELECT realty_platform, COUNT(*) as c
    FROM unified_listings
    WHERE geo_id IS NULL
    GROUP BY realty_platform
    ORDER BY c DESC
  `);
  for (const row of byPlatform.rows) {
    console.log(`  ${row.realty_platform}: ${row.c}`);
  }

  await client.end();
}

main().catch(console.error);
