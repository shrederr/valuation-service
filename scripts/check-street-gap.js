const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Check listings with geo_id but no street_id (candidates for text-based street matching)
  const res = await client.query(`
    SELECT
      realty_platform,
      COUNT(*) as total,
      COUNT(CASE WHEN street_id IS NOT NULL THEN 1 END) as with_street,
      COUNT(CASE WHEN street_id IS NULL THEN 1 END) as no_street
    FROM unified_listings
    WHERE geo_id IS NOT NULL
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  console.log('Listings WITH geo_id:');
  console.log('Platform         | Total    | With Street | No Street');
  console.log('-'.repeat(60));
  for (const row of res.rows) {
    console.log(`${row.realty_platform.padEnd(16)} | ${row.total.toString().padStart(8)} | ${row.with_street.toString().padStart(11)} | ${row.no_street.toString().padStart(9)}`);
  }

  // Total gap
  const gap = await client.query(`
    SELECT COUNT(*) as c FROM unified_listings
    WHERE geo_id IS NOT NULL AND street_id IS NULL
  `);
  console.log('\n\nTotal listings with geo_id but NO street_id:', gap.rows[0].c);

  // Non-OLX gap
  const nonOlxGap = await client.query(`
    SELECT COUNT(*) as c FROM unified_listings
    WHERE geo_id IS NOT NULL AND street_id IS NULL AND realty_platform != 'olx'
  `);
  console.log('Non-OLX listings with geo_id but NO street_id:', nonOlxGap.rows[0].c);

  await client.end();
}

main().catch(console.error);
