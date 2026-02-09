const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

// Blacklist - not residential complexes
const BLACKLIST = new Set([
  'сільпо', 'novus', 'varus', 'атб', 'атб-маркет', 'фора', 'ашан',
  'котельня', 'теплопункт', 'підстанція', 'трансформаторна',
  'ангар', 'склад', 'гараж', 'garage', 'паркінг',
  'продукти', 'продуктовий', 'аптека', 'пошта', 'нова пошта',
  'магазин', 'супермаркет', 'гіпермаркет',
  'поліція', 'військомат', 'комісаріат',
  'школа', 'садок', 'ліцей', 'гімназія', 'університет',
  'церква', 'храм', 'собор', 'мечеть', 'синагога',
  'лікарня', 'поліклініка', 'медичний', 'стоматологія',
  'ресторан', 'кафе', 'бар', 'паб', 'піцерія',
  'корпус', 'будинок', 'секція', 'блок', 'буд',
  'олімп', 'старт', 'динамо', 'спартак',
]);

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected\n');

  // Load all complexes with "ЖК" prefix
  console.log('Loading complexes...');
  const complexesResult = await client.query(`
    SELECT id, name_ru, name_uk, name_normalized
    FROM apartment_complexes
    WHERE (
      name_ru ILIKE 'ЖК %' OR
      name_ru ILIKE 'Житловий комплекс%' OR
      name_uk ILIKE 'ЖК %' OR
      name_uk ILIKE 'Житловий комплекс%'
    )
  `);

  // Build lookup map: normalized name -> complex_id
  const complexMap = new Map();
  for (const c of complexesResult.rows) {
    let name = (c.name_ru || c.name_uk || '')
      .replace(/^ЖК\s+/i, '')
      .replace(/^Житловий комплекс\s+/i, '')
      .replace(/[«»"'()]/g, '')
      .trim()
      .toLowerCase();

    if (name.length < 4 || BLACKLIST.has(name)) continue;

    // Only keep longest ID for duplicate names
    if (!complexMap.has(name) || name.length > 10) {
      complexMap.set(name, c.id);
    }
  }

  console.log(`Loaded ${complexMap.size} unique complex names\n`);

  // Build regex pattern for all complex names
  const sortedNames = [...complexMap.keys()].sort((a, b) => b.length - a.length);
  const escapedNames = sortedNames.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const pattern = new RegExp(`жк\\s+["«]?(${escapedNames.join('|')})["»]?`, 'gi');

  console.log('Pattern built, processing listings...\n');

  // Process listings in batches
  const BATCH_SIZE = 10000;
  let offset = 0;
  let totalMatched = 0;
  let totalProcessed = 0;
  const matchCounts = new Map();

  while (true) {
    const listings = await client.query(`
      SELECT id, description::text as desc
      FROM unified_listings
      WHERE complex_id IS NULL
        AND description IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [BATCH_SIZE, offset]);

    if (listings.rows.length === 0) break;

    const updates = [];

    for (const listing of listings.rows) {
      if (!listing.desc) continue;

      // Find first matching complex name
      pattern.lastIndex = 0;
      const match = pattern.exec(listing.desc);

      if (match) {
        const matchedName = match[1].toLowerCase();
        const complexId = complexMap.get(matchedName);

        if (complexId) {
          updates.push({ id: listing.id, complexId });
          matchCounts.set(matchedName, (matchCounts.get(matchedName) || 0) + 1);
        }
      }
    }

    // Batch update
    if (updates.length > 0) {
      const values = updates.map((u, i) => `($${i*2+1}::uuid, $${i*2+2}::integer)`).join(',');
      const params = updates.flatMap(u => [u.id, u.complexId]);

      await client.query(`
        UPDATE unified_listings AS ul
        SET complex_id = v.complex_id
        FROM (VALUES ${values}) AS v(id, complex_id)
        WHERE ul.id = v.id
      `, params);

      totalMatched += updates.length;
    }

    totalProcessed += listings.rows.length;
    offset += BATCH_SIZE;

    if (totalProcessed % 50000 === 0) {
      console.log(`Processed: ${totalProcessed}, matched: ${totalMatched}`);
    }
  }

  console.log(`\n=== DONE ===`);
  console.log(`Total processed: ${totalProcessed}`);
  console.log(`Total matched: ${totalMatched}\n`);

  // Top matches
  const topMatches = [...matchCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30);

  console.log('Top 30 matches:');
  topMatches.forEach(([name, count]) => {
    console.log(`  ${name}: ${count}`);
  });

  // Final stats
  const stats = await client.query(`
    SELECT realty_platform,
           COUNT(*) as total,
           COUNT(complex_id) as with_complex
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  console.log('\n=== Финальная статистика ===');
  stats.rows.forEach(r => {
    const pct = ((r.with_complex / r.total) * 100).toFixed(1);
    console.log(`${r.realty_platform}: ${r.with_complex}/${r.total} (${pct}%)`);
  });

  await client.end();
}

main().catch(console.error);
