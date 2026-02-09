const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Overall stats by platform
  const stats = await client.query(`
    SELECT
      realty_platform,
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as with_geo,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      ROUND(100.0 * COUNT(*) FILTER (WHERE street_id IS NOT NULL) / COUNT(*), 1) as street_pct
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);
  console.log('Stats by platform:');
  for (const row of stats.rows) {
    console.log(`  ${row.realty_platform || 'null'}: ${row.total} total, ${row.street_pct}% with street`);
  }

  // Total stats
  const total = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as with_geo,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      ROUND(100.0 * COUNT(*) FILTER (WHERE street_id IS NOT NULL) / COUNT(*), 1) as street_pct
    FROM unified_listings
  `);
  console.log('\nTotal:', total.rows[0]);

  await client.end();
}

main().catch(console.error);
