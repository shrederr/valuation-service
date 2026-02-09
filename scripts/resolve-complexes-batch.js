const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to database\n');

  // ========== STEP 1: Coordinate matching in batches ==========
  console.log('=== Coordinate matching (point in polygon) - batched ===\n');

  const BATCH_SIZE = 50000;
  let totalMatched = 0;
  let offset = 0;

  // Get total count
  const countResult = await client.query('SELECT COUNT(*) FROM unified_listings WHERE complex_id IS NULL');
  const total = parseInt(countResult.rows[0].count);
  console.log(`Total listings without complex: ${total}\n`);

  while (true) {
    // Get batch of listing IDs that need matching
    const batchResult = await client.query(`
      WITH batch AS (
        SELECT id, lng, lat
        FROM unified_listings
        WHERE complex_id IS NULL
          AND lat IS NOT NULL
          AND lng IS NOT NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
      )
      SELECT b.id, ac.id as complex_id
      FROM batch b
      JOIN apartment_complexes ac ON ac.polygon IS NOT NULL
        AND ST_Contains(ac.polygon, ST_SetSRID(ST_MakePoint(b.lng, b.lat), 4326))
    `, [BATCH_SIZE, offset]);

    if (batchResult.rows.length === 0) {
      // Check if we've processed everything
      const remaining = await client.query(`
        SELECT COUNT(*) FROM unified_listings
        WHERE complex_id IS NULL AND lat IS NOT NULL
        LIMIT 1 OFFSET $1
      `, [offset]);

      if (parseInt(remaining.rows[0].count) === 0) break;

      offset += BATCH_SIZE;
      continue;
    }

    // Batch update
    if (batchResult.rows.length > 0) {
      const updates = batchResult.rows;
      const values = updates.map((u, i) => `($${i*2+1}::uuid, $${i*2+2}::int)`).join(',');
      const params = updates.flatMap(u => [u.id, u.complex_id]);

      await client.query(`
        UPDATE unified_listings ul
        SET complex_id = v.complex_id
        FROM (VALUES ${values}) AS v(id, complex_id)
        WHERE ul.id = v.id
      `, params);

      totalMatched += updates.length;
    }

    offset += BATCH_SIZE;
    const pct = Math.min(((offset / total) * 100), 100).toFixed(1);
    console.log(`Processed ${offset}/${total} (${pct}%) | Matched: ${totalMatched}`);
  }

  console.log(`\nTotal matched by coordinates: ${totalMatched}\n`);

  // ========== STEP 2: Show final stats ==========
  console.log('=== FINAL STATISTICS ===\n');

  const stats = await client.query(`
    SELECT realty_platform,
           COUNT(*) as total,
           COUNT(complex_id) as with_complex
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  stats.rows.forEach(r => {
    const pct = ((parseInt(r.with_complex) / parseInt(r.total)) * 100).toFixed(1);
    console.log(`${r.realty_platform}: ${r.with_complex} / ${r.total} (${pct}%)`);
  });

  await client.end();
}

main().catch(console.error);
