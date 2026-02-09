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

  // Get all complexes with polygons
  const complexes = await client.query(`
    SELECT id, name_ru
    FROM apartment_complexes
    WHERE polygon IS NOT NULL
    ORDER BY id
  `);
  console.log(`Complexes with polygons: ${complexes.rows.length}\n`);

  let totalMatched = 0;
  let processed = 0;

  console.log('Processing complexes...\n');

  for (const complex of complexes.rows) {
    processed++;

    // Find all listings inside this complex's polygon
    const result = await client.query(`
      UPDATE unified_listings
      SET complex_id = $1
      WHERE complex_id IS NULL
        AND lat IS NOT NULL
        AND lng IS NOT NULL
        AND ST_Contains(
          (SELECT polygon FROM apartment_complexes WHERE id = $1),
          ST_SetSRID(ST_MakePoint(lng, lat), 4326)
        )
    `, [complex.id]);

    if (result.rowCount > 0) {
      totalMatched += result.rowCount;
      console.log(`${complex.name_ru}: ${result.rowCount} listings`);
    }

    // Progress every 500 complexes
    if (processed % 500 === 0) {
      console.log(`\n--- Progress: ${processed}/${complexes.rows.length} complexes, ${totalMatched} listings matched ---\n`);
    }
  }

  console.log(`\n=== DONE ===`);
  console.log(`Processed ${processed} complexes`);
  console.log(`Total matched: ${totalMatched}\n`);

  // Final stats
  const stats = await client.query(`
    SELECT realty_platform,
           COUNT(*) as total,
           COUNT(complex_id) as with_complex
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  console.log('=== FINAL STATISTICS ===\n');
  stats.rows.forEach(r => {
    const pct = ((parseInt(r.with_complex) / parseInt(r.total)) * 100).toFixed(1);
    console.log(`${r.realty_platform}: ${r.with_complex} / ${r.total} (${pct}%)`);
  });

  await client.end();
}

main().catch(console.error);
