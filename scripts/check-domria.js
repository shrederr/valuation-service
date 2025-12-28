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

  // Check table structure first
  const cols = await ds.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'unified_listings'
    ORDER BY ordinal_position
  `);
  console.log('Columns:', cols.map(c => c.column_name).join(', '));

  // Check by external_url
  const byUrl = await ds.query(`
    SELECT
      CASE
        WHEN external_url LIKE '%olx%' THEN 'olx'
        WHEN external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%' THEN 'domria'
        WHEN external_url LIKE '%rieltor%' THEN 'rieltor'
        ELSE 'other'
      END as platform,
      COUNT(*) as cnt
    FROM unified_listings
    WHERE external_url IS NOT NULL
    GROUP BY 1
    ORDER BY cnt DESC
  `);
  console.log('\nBy URL:', byUrl);

  // Get dom.ria samples with ЖК in description
  const samples = await ds.query(`
    SELECT id, description::text as description, external_url
    FROM unified_listings
    WHERE (external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%')
      AND description IS NOT NULL
      AND (
        description::text ILIKE '%жк %' OR
        description::text ILIKE '%жк"%' OR
        description::text ILIKE '%жк«%' OR
        description::text ILIKE '%житловий комплекс%' OR
        description::text ILIKE '%жилой комплекс%' OR
        description::text ILIKE '%ж.к.%'
      )
    LIMIT 50
  `);

  console.log('\n=== Sample dom.ria listings ===\n');
  console.log('Found:', samples.length, 'samples\n');

  // Look for ЖК patterns - more comprehensive
  const jkPattern = /(?:жк|ж\.к\.|жилой комплекс|житловий комплекс)\s*[«"']?([^«"'.,\n]{3,30})[»"']?/gi;
  const quotedPattern = /[«"']([^«"']{3,40})[»"']/g;

  let foundCount = 0;

  samples.forEach((s, i) => {
    const text = String(s.description || '');

    // Find ЖК mentions
    const jkMatches = [...text.matchAll(jkPattern)];
    const quotedMatches = [...text.matchAll(quotedPattern)];

    if (jkMatches.length > 0 || text.toLowerCase().includes('жк') || text.toLowerCase().includes('комплекс')) {
      foundCount++;
      console.log(`--- ${foundCount} ---`);
      console.log('URL:', s.external_url);
      console.log('Desc:', text.substring(0, 400));
      if (jkMatches.length > 0) {
        console.log('>>> ЖК FOUND:', jkMatches.map(m => m[0]).join(' | '));
      }
      console.log('');
    }
  });

  console.log('\n=== Summary ===');
  console.log('Total samples:', samples.length);
  console.log('With ЖК mentions:', foundCount);

  await ds.destroy();
}

main().catch(console.error);
