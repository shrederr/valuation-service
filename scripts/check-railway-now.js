const { Client } = require('pg');
const fs = require('fs');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Проверим текущее состояние
  const result = await client.query(`
    SELECT id, name, names
    FROM streets
    WHERE id IN (34399, 28305, 31665, 34338)
  `);

  fs.writeFileSync('output/railway-now.json', JSON.stringify(result.rows, null, 2));
  console.log('Сохранено в output/railway-now.json');

  await client.end();
}

main().catch(console.error);
