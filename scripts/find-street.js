const { Client } = require('pg');

async function main() {
  const searchTerm = process.argv[2] || 'змієнк';

  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  const result = await client.query(`
    SELECT s.id, s.name, s.names, g.name->>'uk' as city
    FROM streets s
    LEFT JOIN geo g ON s.geo_id = g.id
    WHERE s.name::text ILIKE $1 OR s.names::text ILIKE $1
  `, [`%${searchTerm}%`]);

  console.log(`Поиск: "${searchTerm}"`);
  console.log(`Найдено: ${result.rows.length}\n`);

  result.rows.forEach(r => {
    console.log(`ID: ${r.id}`);
    console.log(`name: ${JSON.stringify(r.name)}`);
    console.log(`names: ${JSON.stringify(r.names)}`);
    console.log(`city: ${r.city}`);
    console.log('---');
  });

  await client.end();
}

main().catch(console.error);
