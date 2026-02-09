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

  const result = await client.query(`
    SELECT id, name, names
    FROM streets
    WHERE id IN (34294, 34537, 37358)
  `);

  console.log('Локальная база - улицы по ID:');
  result.rows.forEach(r => {
    console.log(`\nID: ${r.id}`);

    // Fix encoding for name
    const nameUk = r.name?.uk ? fixEncoding(r.name.uk) : null;
    const nameRu = r.name?.ru ? fixEncoding(r.name.ru) : null;
    console.log(`name.uk: ${nameUk}`);
    console.log(`name.ru: ${nameRu}`);

    // Fix encoding for names
    if (r.names?.uk) {
      const namesUk = r.names.uk.map(n => fixEncoding(n));
      console.log(`names.uk: ${JSON.stringify(namesUk)}`);
    }
    if (r.names?.ru) {
      const namesRu = r.names.ru.map(n => fixEncoding(n));
      console.log(`names.ru: ${JSON.stringify(namesRu)}`);
    }
  });

  await client.end();
}

main().catch(console.error);
