const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('Connected to Railway DB');

  // Get count of listings needing streets
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE street_id IS NULL AND lat IS NOT NULL AND lng IS NOT NULL
  `);
  const total = parseInt(countResult.rows[0].cnt);
  console.log('Listings needing streets:', total);

  const CHUNK_SIZE = 10000;
  let processed = 0;
  let totalUpdated = 0;
  const startTime = Date.now();

  while (processed < total) {
    const chunkStart = Date.now();

    // Process a chunk using LIMIT
    const result = await client.query(`
      UPDATE unified_listings ul
      SET street_id = nearest.street_id
      FROM (
        SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
        FROM (
          SELECT id, lng, lat
          FROM unified_listings
          WHERE street_id IS NULL AND lat IS NOT NULL AND lng IS NOT NULL
          LIMIT ${CHUNK_SIZE}
        ) ul2
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
      ) nearest
      WHERE ul.id = nearest.listing_id
    `);

    const updated = result.rowCount;
    totalUpdated += updated;
    processed += CHUNK_SIZE;

    const elapsed = ((Date.now() - chunkStart) / 1000).toFixed(1);
    const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`Chunk: +${updated} streets (${elapsed}s) | Total: ${totalUpdated} | Progress: ${Math.min(processed, total)}/${total} | Time: ${totalElapsed}min`);

    // If no updates in this chunk, we've processed all that can be matched
    if (updated === 0) {
      console.log('No more matches found, stopping.');
      break;
    }
  }

  console.log(`\nDone! Total streets assigned: ${totalUpdated}`);

  // Final stats
  const final = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
  `);
  console.log('Final:', final.rows[0]);

  await client.end();
}

main().catch(console.error);
