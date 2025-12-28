const { DataSource } = require('typeorm');
require('dotenv').config();

const ds = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
});

async function main() {
  await ds.initialize();

  // Count dom.ria listings with ЖК mentions
  const withJK = await ds.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE (external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%')
      AND (
        description::text ILIKE '%жк %' OR
        description::text ILIKE '%жк"%' OR
        description::text ILIKE '%жк«%' OR
        description::text ILIKE '%житловий комплекс%' OR
        description::text ILIKE '%жилой комплекс%' OR
        description::text ILIKE '%ж.к.%'
      )
  `);

  const total = await ds.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE (external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%')
  `);

  console.log('Dom.ria statistics:');
  console.log('  Total listings:', total[0].cnt);
  console.log('  With ЖК mentions:', withJK[0].cnt);
  console.log('  Percentage:', ((withJK[0].cnt / total[0].cnt) * 100).toFixed(1) + '%');

  // Get all unique ЖК names from dom.ria
  console.log('\n\n=== Extracting ЖК names ===\n');

  const samples = await ds.query(`
    SELECT id, description::text as description
    FROM unified_listings
    WHERE (external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%')
      AND (
        description::text ILIKE '%жк %' OR
        description::text ILIKE '%жк"%' OR
        description::text ILIKE '%жк«%'
      )
    LIMIT 500
  `);

  // Extract ЖК names
  const complexNames = new Map();
  const jkPattern = /(?:жк|ж\.к\.)\s*[«"']?([а-яіїєґa-z0-9\s\-]+)/gi;

  for (const s of samples) {
    const text = String(s.description || '');
    let match;
    while ((match = jkPattern.exec(text)) !== null) {
      let name = match[1].trim();
      // Clean up
      name = name.replace(/^[\s,.\-:]+|[\s,.\-:]+$/g, '');
      // Skip too short or common words
      if (name.length < 3) continue;
      if (/^(на|в|с|от|по|для|это|эт|от|и|или|с|для)$/i.test(name)) continue;
      // Skip if starts with common stop words
      if (/^(на\s|в\s|с\s|от\s|по\s)/i.test(name)) continue;

      // Normalize - take first 2-3 words
      const words = name.split(/\s+/).slice(0, 3);
      name = words.join(' ');

      if (name.length >= 3 && name.length <= 40) {
        const key = name.toLowerCase();
        if (!complexNames.has(key)) {
          complexNames.set(key, { name, count: 0 });
        }
        complexNames.get(key).count++;
      }
    }
  }

  // Sort by count
  const sorted = [...complexNames.values()].sort((a, b) => b.count - a.count);

  console.log('Top 50 most mentioned ЖК names from dom.ria:\n');
  sorted.slice(0, 50).forEach((c, i) => {
    console.log(`  ${i+1}. "${c.name}" (${c.count} mentions)`);
  });

  // Check how many are already in our database
  console.log('\n\n=== Matching with apartment_complexes ===\n');

  const existing = await ds.query(`SELECT id, name_ru, name_uk FROM apartment_complexes`);
  const existingNormalized = new Set();
  existing.forEach(e => {
    existingNormalized.add(e.name_ru?.toLowerCase().replace(/жк\s*/gi, '').trim());
    existingNormalized.add(e.name_uk?.toLowerCase().replace(/жк\s*/gi, '').trim());
  });

  let matched = 0;
  let notMatched = [];
  sorted.forEach(c => {
    const normalized = c.name.toLowerCase();
    if (existingNormalized.has(normalized)) {
      matched++;
    } else {
      notMatched.push(c);
    }
  });

  console.log('Matched with existing complexes:', matched);
  console.log('Not matched:', notMatched.length);

  console.log('\nTop 20 NOT matched (may need to add or fix matching):');
  notMatched.slice(0, 20).forEach((c, i) => {
    console.log(`  ${i+1}. "${c.name}" (${c.count} mentions)`);
  });

  await ds.destroy();
}

main().catch(console.error);
