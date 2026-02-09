const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Проверим как хранятся данные
  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets
    WHERE id = 28305
  `);

  console.log('Raw result:');
  console.log(result.rows[0]);

  console.log('\nname_uk:', result.rows[0].name_uk);
  console.log('uk_names:', result.rows[0].uk_names);

  // Попробуем найти Дворянську напрямую
  const dvoryan = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets
    WHERE name->>'uk' ILIKE '%дворян%'
    LIMIT 5
  `);

  console.log('\nПоиск Дворянська:');
  dvoryan.rows.forEach(r => {
    console.log(`[${r.id}] ${r.name_uk}`);
    console.log('  names:', r.uk_names);
  });

  await client.end();
}

main().catch(console.error);
