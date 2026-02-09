const { Client } = require('pg');
const fs = require('fs');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Проверим разные записи
  const result = await client.query(`
    SELECT id, names->'uk'->>0 as first_name,
           encode(names->'uk'->>0::bytea, 'hex') as hex_bytes
    FROM streets
    WHERE id IN (28305, 34399)
  `);

  console.log('Записи:');
  result.rows.forEach(r => {
    console.log(`[${r.id}] "${r.first_name}"`);
    console.log(`  hex: ${r.hex_bytes}`);
  });

  // Попробуем fix_encoding напрямую для 34399
  const test = await client.query(`
    SELECT
      names->'uk'->>0 as original,
      fix_encoding(names->'uk'->>0) as fixed
    FROM streets WHERE id = 34399
  `);

  console.log('\nТест fix_encoding для 34399:');
  console.log('Original:', test.rows[0].original);
  console.log('Fixed:', test.rows[0].fixed);

  await client.end();
}
main().catch(console.error);
