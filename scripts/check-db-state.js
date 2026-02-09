const { Client } = require('pg');
const fs = require('fs');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE id IN (34399, 28305, 31665)
  `);

  // Записываем в файл чтобы избежать проблем с консолью
  fs.writeFileSync('output/db-state.json', JSON.stringify(result.rows, null, 2));
  console.log('Записано в output/db-state.json');

  await client.end();
}
main().catch(console.error);
