const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('Connected to Railway DB');

  // Check current status
  const before = await client.query(`
    SELECT
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NOT NULL) as no_street_with_geo,
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NULL AND lat IS NOT NULL) as no_geo_no_street
    FROM unified_listings
  `);
  console.log('Before:', before.rows[0]);

  console.log('\nRunning SQL street resolution (without geo_id restriction, 300m radius)...');
  console.log('This may take several minutes...\n');

  const startTime = Date.now();

  // Broader search - no geo_id restriction, 300m radius
  const result = await client.query(`
    UPDATE unified_listings ul
    SET street_id = nearest.street_id
    FROM (
      SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
      FROM unified_listings ul2
      CROSS JOIN LATERAL (
        SELECT s.id
        FROM streets s
        WHERE s.line IS NOT NULL
          AND ST_DWithin(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
            300
          )
        ORDER BY ST_Distance(
          s.line::geography,
          ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
        )
        LIMIT 1
      ) s
      WHERE ul2.street_id IS NULL
        AND ul2.lat IS NOT NULL
        AND ul2.lng IS NOT NULL
    ) nearest
    WHERE ul.id = nearest.listing_id
  `);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Done in ${elapsed}s, rows affected: ${result.rowCount}`);

  // Check status after
  const after = await client.query(`
    SELECT
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NOT NULL) as no_street_with_geo,
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NULL AND lat IS NOT NULL) as no_geo_no_street
    FROM unified_listings
  `);
  console.log('After:', after.rows[0]);

  // Show breakdown by platform
  const byPlatform = await client.query(`
    SELECT
      realty_platform,
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);
  console.log('\nBy platform:');
  for (const row of byPlatform.rows) {
    const pct = ((row.with_street / row.total) * 100).toFixed(1);
    console.log(`  ${row.realty_platform}: ${row.with_street}/${row.total} (${pct}%)`);
  }

  await client.end();
}

main().catch(console.error);
