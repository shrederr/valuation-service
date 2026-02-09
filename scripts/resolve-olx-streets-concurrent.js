const { Pool } = require('pg');

async function main() {
  const pool = new Pool({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation',
    max: 10, // 10 concurrent connections
  });

  console.log('=== OLX Street Resolution - Concurrent Mode ===\n');

  const radiusMeters = parseInt(process.argv[2] || '200', 10);
  const concurrency = parseInt(process.argv[3] || '10', 10);
  const batchFetch = parseInt(process.argv[4] || '500', 10);

  console.log(`Radius: ${radiusMeters}m, Concurrency: ${concurrency}, Batch fetch: ${batchFetch}`);

  const client = await pool.connect();

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
  console.log(`Remaining to process: ${totalRemaining}\n`);

  client.release();

  let processed = 0;
  let updated = 0;
  let offset = 0;
  const startTime = Date.now();

  while (processed < totalRemaining) {
    // Fetch a batch of listings to process
    const fetchClient = await pool.connect();
    const listings = await fetchClient.query(`
      SELECT id, lng, lat
      FROM unified_listings
      WHERE realty_platform = 'olx' AND street_id IS NULL
        AND lat IS NOT NULL AND lng IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchFetch, offset]);
    fetchClient.release();

    if (listings.rows.length === 0) break;

    // Process in parallel with limited concurrency
    const chunks = [];
    for (let i = 0; i < listings.rows.length; i += concurrency) {
      chunks.push(listings.rows.slice(i, i + concurrency));
    }

    for (const chunk of chunks) {
      const promises = chunk.map(async (listing) => {
        const updateClient = await pool.connect();
        try {
          const result = await updateClient.query(`
            UPDATE unified_listings ul
            SET street_id = (
              SELECT s.id
              FROM streets s
              WHERE s.line IS NOT NULL
                AND ST_DWithin(
                  s.line::geography,
                  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                  $3
                )
              ORDER BY ST_Distance(
                s.line::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
              )
              LIMIT 1
            )
            WHERE ul.id = $4::uuid AND ul.street_id IS NULL
            RETURNING ul.id
          `, [listing.lng, listing.lat, radiusMeters, listing.id]);
          return result.rowCount > 0 ? 1 : 0;
        } finally {
          updateClient.release();
        }
      });

      const results = await Promise.all(promises);
      updated += results.reduce((a, b) => a + b, 0);
      processed += chunk.length;
    }

    offset += batchFetch;

    const elapsed = Math.max(0.1, (Date.now() - startTime) / 1000 / 60);
    const rate = Math.round(processed / elapsed);
    console.log(
      `Progress: ${Math.min(processed, totalRemaining)}/${totalRemaining} (${((Math.min(processed, totalRemaining)/totalRemaining)*100).toFixed(1)}%) | ` +
      `Updated: ${updated} | ${elapsed.toFixed(1)}min (${rate}/min)`
    );
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nCompleted in ${elapsed} minutes, updated ${updated} listings`);

  const afterClient = await pool.connect();
  const after = await afterClient.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('After:', after.rows[0]);
  afterClient.release();

  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
