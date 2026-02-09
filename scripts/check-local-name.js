const { Client } = require('pg');
const fs = require('fs');

// ТОЛЬКО ЧТЕНИЕ!

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  await client.connect();

  const result = await client.query(`
    SELECT id, name, names
    FROM streets
    WHERE id IN (34399, 28305, 31665, 34338)
  `);

  fs.writeFileSync('output/local-name-full.json', JSON.stringify(result.rows, null, 2));
  console.log('Сохранено в output/local-name-full.json');

  await client.end();
}

main().catch(console.error);
