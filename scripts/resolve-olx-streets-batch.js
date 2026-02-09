const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation',
    statement_timeout: 120000, // 2 minute timeout per query
  });

  await client.connect();
  console.log('=== OLX Street Resolution - Batch Mode ===\n');

  const radiusMeters = parseInt(process.argv[2] || '200', 10);
  const batchSize = parseInt(process.argv[3] || '100', 10);

  console.log(`Radius: ${radiusMeters}m, Batch size: ${batchSize}`);

  const before = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('Before:', before.rows[0]);

  // Count remaining
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE realty_platform = 'olx' AND street_id IS NULL
      AND lat IS NOT NULL AND lng IS NOT NULL
  `);
  const totalRemaining = parseInt(countResult.rows[0].cnt, 10);
  console.log(`\nRemaining to process: ${totalRemaining}`);

  let processed = 0;
  let updated = 0;
  let offset = 0;
  const startTime = Date.now();

  while (processed < totalRemaining) {
    // Process in batches
    const result = await client.query(`
      WITH batch AS (
        SELECT id, lng, lat
        FROM unified_listings
        WHERE realty_platform = 'olx' AND street_id IS NULL
          AND lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
      )
      UPDATE unified_listings ul
      SET street_id = nearest.street_id
      FROM (
        SELECT DISTINCT ON (b.id) b.id as listing_id, s.id as street_id
        FROM batch b
        CROSS JOIN LATERAL (
          SELECT s.id
          FROM streets s
          WHERE s.line IS NOT NULL
            AND ST_DWithin(
              s.line::geography,
              ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326)::geography,
              $3
            )
          ORDER BY ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326)::geography
          )
          LIMIT 1
        ) s
      ) nearest
      WHERE ul.id = nearest.listing_id
      RETURNING ul.id
    `, [batchSize, offset, radiusMeters]);

    const batchUpdated = result.rowCount || 0;
    updated += batchUpdated;
    processed += batchSize;
    offset += batchSize;

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const rate = Math.round(processed / parseFloat(elapsed || 0.1));
    console.log(
      `Progress: ${Math.min(processed, totalRemaining)}/${totalRemaining} (${((Math.min(processed, totalRemaining)/totalRemaining)*100).toFixed(1)}%) | ` +
      `Updated: ${updated} | ${elapsed}min (${rate}/min)`
    );

    if (batchUpdated === 0 && processed < totalRemaining) {
      // No updates in this batch, likely no streets nearby for these listings
      // Continue to next batch
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nCompleted in ${elapsed} minutes, updated ${updated} listings`);

  const after = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('After:', after.rows[0]);

  await client.end();
}

main().catch(e => { console.error(e); process.exit(1); });
