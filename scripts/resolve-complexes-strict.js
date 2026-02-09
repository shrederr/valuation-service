const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

// Blacklist - these are NOT residential complexes
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
  'олімп', 'старт', 'динамо', 'спартак', // sports names
]);

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected\n');

  // Get complexes that:
  // 1. Have "ЖК" or "Житловий комплекс" in name
  // 2. Name is at least 5 chars (after removing prefix)
  // 3. Not in blacklist
  const complexes = await client.query(`
    SELECT id, name_ru, name_uk, name_normalized
    FROM apartment_complexes
    WHERE (
      name_ru ILIKE 'ЖК %' OR
      name_ru ILIKE 'Житловий комплекс%' OR
      name_uk ILIKE 'ЖК %' OR
      name_uk ILIKE 'Житловий комплекс%'
    )
    ORDER BY LENGTH(name_normalized) DESC
  `);

  console.log(`Загружено ${complexes.rows.length} ЖК с префиксом\n`);

  let totalMatched = 0;
  let processed = 0;

  for (const complex of complexes.rows) {
    // Extract name without prefix
    let searchName = complex.name_ru || complex.name_uk || '';
    searchName = searchName
      .replace(/^ЖК\s+/i, '')
      .replace(/^Житловий комплекс\s+/i, '')
      .replace(/[«»"']/g, '')
      .trim();

    // Skip short names and blacklisted
    if (searchName.length < 4) continue;
    if (BLACKLIST.has(searchName.toLowerCase())) continue;

    // Escape for LIKE
    const escaped = searchName.replace(/%/g, '\\%').replace(/_/g, '\\_');

    // Search for "ЖК <name>" pattern only (requires explicit ЖК prefix)
    const result = await client.query(`
      UPDATE unified_listings
      SET complex_id = $1
      WHERE complex_id IS NULL
        AND (
          description::text ILIKE $2 OR
          description::text ILIKE $3 OR
          description::text ILIKE $4
        )
    `, [
      complex.id,
      `%жк ${escaped}%`,
      `%жк "${escaped}"%`,
      `%жк «${escaped}»%`
    ]);

    if (result.rowCount > 0) {
      totalMatched += result.rowCount;
      console.log(`${complex.name_ru}: +${result.rowCount}`);
    }

    processed++;
    if (processed % 500 === 0) {
      console.log(`--- ${processed} обработано, matched: ${totalMatched} ---`);
    }
  }

  console.log(`\n=== ИТОГО ===`);
  console.log(`Matched by strict "ЖК <name>": ${totalMatched}\n`);

  // Stats by platform
  const stats = await client.query(`
    SELECT realty_platform,
           COUNT(*) as total,
           COUNT(complex_id) as with_complex
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  console.log('=== Финальная статистика ===');
  stats.rows.forEach(r => {
    const pct = ((r.with_complex / r.total) * 100).toFixed(1);
    console.log(`${r.realty_platform}: ${r.with_complex}/${r.total} (${pct}%)`);
  });

  await client.end();
}

main().catch(console.error);
