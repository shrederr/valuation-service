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

  // Get complexes sorted by name length (longer names first to avoid partial matches)
  const complexes = await client.query(`
    SELECT id, name_ru, name_uk, name_normalized
    FROM apartment_complexes
    WHERE LENGTH(name_normalized) >= 4
    ORDER BY LENGTH(name_normalized) DESC
    LIMIT 5000
  `);
  console.log(`Processing ${complexes.rows.length} complexes\n`);

  let totalMatched = 0;

  for (let i = 0; i < complexes.rows.length; i++) {
    const complex = complexes.rows[i];
    const names = [complex.name_normalized, complex.name_ru, complex.name_uk].filter(Boolean);

    for (const name of names) {
      if (name.length < 4) continue;

      // Escape special chars for LIKE
      const escaped = name.replace(/%/g, '\\%').replace(/_/g, '\\_');

      // Search in description JSON for this complex name
      const result = await client.query(`
        UPDATE unified_listings
        SET complex_id = $1
        WHERE complex_id IS NULL
          AND (
            description::text ILIKE $2
            OR description::text ILIKE $3
          )
      `, [complex.id, `%жк%${escaped}%`, `%${escaped}%`]);

      if (result.rowCount > 0) {
        totalMatched += result.rowCount;
        console.log(`${complex.name_ru}: +${result.rowCount} (total: ${totalMatched})`);
        break; // Don't try other name variants for this complex
      }
    }

    if ((i + 1) % 100 === 0) {
      console.log(`--- Progress: ${i + 1}/${complexes.rows.length} complexes, ${totalMatched} matched ---`);
    }
  }

  console.log(`\n=== DONE ===`);
  console.log(`Total matched by text: ${totalMatched}\n`);

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
