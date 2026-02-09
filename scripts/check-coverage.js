const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  const result = await client.query(`
    SELECT
      realty_platform,
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(geo_id) as with_geo,
      ROUND(COUNT(street_id)::numeric / COUNT(*)::numeric * 100, 1) as street_pct,
      ROUND(COUNT(geo_id)::numeric / COUNT(*)::numeric * 100, 1) as geo_pct
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  console.log('=== Coverage by Platform ===');
  console.log('Platform'.padEnd(15), 'Total'.padStart(10), 'Street%'.padStart(10), 'Geo%'.padStart(10));
  console.log('-'.repeat(45));
  for (const r of result.rows) {
    console.log(
      (r.realty_platform || 'null').padEnd(15),
      String(r.total).padStart(10),
      (r.street_pct + '%').padStart(10),
      (r.geo_pct + '%').padStart(10)
    );
  }

  const totals = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(geo_id) as with_geo
    FROM unified_listings
  `);

  console.log('-'.repeat(45));
  console.log(
    'TOTAL'.padEnd(15),
    String(totals.rows[0].total).padStart(10),
    ((totals.rows[0].with_street / totals.rows[0].total * 100).toFixed(1) + '%').padStart(10),
    ((totals.rows[0].with_geo / totals.rows[0].total * 100).toFixed(1) + '%').padStart(10)
  );

  await client.end();
}

main().catch(console.error);
