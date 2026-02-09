const { Client } = require('pg');

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
    WHERE name::text ILIKE '%змієнк%' OR names::text ILIKE '%змієнк%'
  `);

  console.log('Локальная база - улицы с "Змієнка":');
  result.rows.forEach(r => {
    console.log(`\nID: ${r.id}`);
    console.log(`name: ${JSON.stringify(r.name)}`);
    console.log(`names: ${JSON.stringify(r.names)}`);
  });

  await client.end();
}

main().catch(console.error);
