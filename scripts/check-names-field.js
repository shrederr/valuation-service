const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Проверка поля names в streets ===\n');

  // Сколько улиц с заполненным names
  const stats = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE names IS NOT NULL AND names::text != 'null' AND names::text != '[]' AND names::text != '{}') as with_names
    FROM streets
  `);
  console.log(`Всего улиц: ${stats.rows[0].total}`);
  console.log(`С заполненным names: ${stats.rows[0].with_names}`);

  // Примеры содержимого names
  const examples = await client.query(`
    SELECT id, name->>'uk' as name_uk, names
    FROM streets
    WHERE names IS NOT NULL
      AND names::text != 'null'
      AND names::text != '[]'
      AND names::text != '{}'
      AND LENGTH(names::text) > 5
    LIMIT 30
  `);

  console.log(`\n--- Примеры содержимого names (${examples.rows.length}) ---`);
  examples.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk}`);
    console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  // Проверим улицы Одессы с names
  const odessaNames = await client.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.names, g.name->>'uk' as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE s.names IS NOT NULL
      AND s.names::text != 'null'
      AND s.names::text != '[]'
      AND LENGTH(s.names::text) > 5
      AND g.name->>'uk' ILIKE '%одес%'
    LIMIT 30
  `);

  console.log(`\n--- Улицы Одессы с names (${odessaNames.rows.length}) ---`);
  odessaNames.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk} | geo: ${s.geo_name}`);
    console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  await client.end();
}

main().catch(console.error);
