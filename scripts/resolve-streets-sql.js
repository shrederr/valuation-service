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
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NOT NULL) as no_street_with_geo
    FROM unified_listings
  `);
  console.log('Before:', before.rows[0]);

  console.log('\nRunning SQL street resolution (LATERAL join)...');
  console.log('This may take several minutes...\n');

  const startTime = Date.now();

  // Fast SQL approach from sync-aggregator-bulk.ts Phase 3
  // Using nearest street within 200m for listings with geo_id
  const result = await client.query(`
    UPDATE unified_listings ul
    SET street_id = nearest.street_id
    FROM (
      SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
      FROM unified_listings ul2
      CROSS JOIN LATERAL (
        SELECT s.id
        FROM streets s
        WHERE s.geo_id = ul2.geo_id
          AND ST_DWithin(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
            200
          )
        ORDER BY ST_Distance(
          s.line::geography,
          ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
        )
        LIMIT 1
      ) s
      WHERE ul2.street_id IS NULL
        AND ul2.geo_id IS NOT NULL
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
      COUNT(*) FILTER (WHERE street_id IS NULL AND geo_id IS NOT NULL) as no_street_with_geo
    FROM unified_listings
  `);
  console.log('After:', after.rows[0]);

  await client.end();
}

main().catch(console.error);
