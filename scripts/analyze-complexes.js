const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Sample of complex names
  console.log('=== Sample complex names ===');
  const sample = await client.query('SELECT id, name_ru, name_uk, source FROM apartment_complexes LIMIT 20');
  for (const s of sample.rows) {
    console.log(`[${s.source}] ${s.name_ru} / ${s.name_uk}`);
  }

  // Count by source
  console.log('\n=== By source ===');
  const sources = await client.query('SELECT source, COUNT(*) as cnt FROM apartment_complexes GROUP BY source ORDER BY cnt DESC');
  for (const s of sources.rows) console.log(`${s.source}: ${s.cnt}`);

  // Check for suspicious names (not residential complexes)
  console.log('\n=== Potentially NOT residential complexes ===');
  const suspicious = await client.query(`
    SELECT name_ru, name_uk FROM apartment_complexes
    WHERE name_ru ~* '(магазин|школа|садок|лікарня|ресторан|кафе|церква|храм|банк|пошта|аптека|склад|гараж|паркінг|стоянка|котельня|підстанція|теплопункт|трансформатор|офіс|бізнес.центр|торгов|супермаркет|гіпермаркет|продукти|сільпо|атб|novus|ашан)'
       OR name_uk ~* '(магазин|школа|садок|лікарня|ресторан|кафе|церква|храм|банк|пошта|аптека|склад|гараж|паркінг|стоянка|котельня|підстанція|теплопункт|трансформатор|офіс|бізнес.центр|торгов|супермаркет|гіпермаркет|продукти|сільпо|атб|novus|ашан)'
    LIMIT 30
  `);
  console.log('Suspicious count:', suspicious.rows.length);
  for (const s of suspicious.rows) console.log(`  ${s.name_ru}`);

  // Count by prefix type
  console.log('\n=== By prefix ===');
  const prefixes = await client.query(`
    SELECT
      CASE
        WHEN name_ru ~* '^ЖК\\s' OR name_uk ~* '^ЖК\\s' THEN 'ЖК'
        WHEN name_ru ~* '^КГ\\s' OR name_uk ~* '^КГ\\s' THEN 'КГ (котеджне)'
        WHEN name_ru ~* '^КМ\\s' OR name_uk ~* '^КМ\\s' THEN 'КМ (котеджне)'
        WHEN name_ru ~* '^Житловий комплекс' OR name_ru ~* '^Жилой комплекс' THEN 'Житловий комплекс'
        WHEN name_ru ~* '^Котедж' OR name_uk ~* '^Котедж' THEN 'Котедж'
        WHEN name_ru ~* '^Таунхаус' OR name_uk ~* '^Таунхаус' THEN 'Таунхаус'
        ELSE 'Other'
      END as prefix_type,
      COUNT(*) as cnt
    FROM apartment_complexes
    GROUP BY prefix_type
    ORDER BY cnt DESC
  `);
  for (const p of prefixes.rows) console.log(`${p.prefix_type}: ${p.cnt}`);

  // Show some "Other" examples
  console.log('\n=== "Other" examples (no ЖК/КГ prefix) ===');
  const other = await client.query(`
    SELECT name_ru, name_uk FROM apartment_complexes
    WHERE name_ru !~* '^(ЖК|КГ|КМ|Житловий комплекс|Жилой комплекс|Котедж|Таунхаус)'
      AND name_uk !~* '^(ЖК|КГ|КМ|Житловий комплекс|Жилой комплекс|Котедж|Таунхаус)'
    LIMIT 30
  `);
  for (const o of other.rows) console.log(`  ${o.name_ru} / ${o.name_uk}`);

  // Short names (potential issues)
  console.log('\n=== Very short names (< 4 chars after removing prefix) ===');
  const short = await client.query(`
    SELECT name_ru, name_uk FROM apartment_complexes
    WHERE LENGTH(REGEXP_REPLACE(name_ru, '^(ЖК|КГ|КМ)\\s+', '', 'i')) < 4
    LIMIT 20
  `);
  for (const s of short.rows) console.log(`  ${s.name_ru}`);

  await client.end();
}

main().catch(console.error);
