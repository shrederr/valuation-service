const { Client } = require('pg');
const iconv = require('iconv-lite');

function fixEncoding(str) {
  if (!str) return str;
  try {
    const buf = iconv.encode(str, 'win1251');
    return iconv.decode(buf, 'utf8');
  } catch (e) {
    return str;
  }
}

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  await client.connect();

  // Читаем данные из локальной базы
  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE id = 34399
  `);

  const row = result.rows[0];
  console.log('Исходные данные из локальной базы:');
  console.log('name_uk:', row.name_uk);
  console.log('names_uk:', row.names_uk);

  console.log('\nПосле fix_encoding:');
  console.log('name_uk:', fixEncoding(row.name_uk));
  console.log('names_uk:', row.names_uk.map(n => fixEncoding(n)));

  await client.end();
}

main().catch(console.error);
