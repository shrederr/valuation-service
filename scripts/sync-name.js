const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  console.log('Синхронизирую name из names[0]...');
  const result = await client.query(`
    UPDATE streets
    SET name = jsonb_build_object(
      'uk', names->'uk'->>0,
      'ru', COALESCE(names->'ru'->>0, ''),
      'en', COALESCE(name->>'en', '')
    )
    WHERE names IS NOT NULL
      AND jsonb_array_length(names->'uk') > 0
      AND name->>'uk' IS DISTINCT FROM names->'uk'->>0
  `);

  console.log('Обновлено:', result.rowCount);

  // Проверка
  const check = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets WHERE id IN (28305, 34399)
  `);
  check.rows.forEach(r => {
    console.log(`[${r.id}] ${r.name_uk}`);
    console.log(`  names: ${JSON.stringify(r.uk_names)}`);
  });

  await client.end();
}
main().catch(console.error);
