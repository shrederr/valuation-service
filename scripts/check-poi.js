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

  // Check for metro stations
  const metros = await client.query(`
    SELECT name_uk, name_ru
    FROM apartment_complexes
    WHERE LOWER(COALESCE(name_uk, name_ru, '')) LIKE '%метро%'
       OR LOWER(COALESCE(name_uk, name_ru, '')) LIKE '%станці%'
    LIMIT 30
  `);

  console.log('=== Станции метро в apartment_complexes ===');
  metros.rows.forEach(r => console.log('-', r.name_uk || r.name_ru));
  console.log('Total:', metros.rows.length);

  // Count POI-like entries by pattern
  const patterns = [
    { name: 'metro', patterns: ['%метро%', '%станці%'] },
    { name: 'supermarket', patterns: ['%сільпо%', '%атб%', '%novus%', '%varus%', '%фора%', '%маркет%'] },
    { name: 'school', patterns: ['%школа%', '%ліцей%', '%гімназі%'] },
    { name: 'kindergarten', patterns: ['%садок%', '%садочок%', '%дитячий сад%'] },
    { name: 'hospital', patterns: ['%лікарн%', '%поліклінік%', '%госпіталь%'] },
    { name: 'park', patterns: ['%парк %', '%сквер%'] },
  ];

  console.log('\n=== POI categories в apartment_complexes ===');
  for (const p of patterns) {
    const whereClause = p.patterns.map((pat, i) =>
      `LOWER(COALESCE(name_uk, name_ru, '')) LIKE '${pat}'`
    ).join(' OR ');

    const result = await client.query(`
      SELECT COUNT(*) as cnt FROM apartment_complexes WHERE ${whereClause}
    `);
    console.log(`${p.name}: ${result.rows[0].cnt}`);
  }

  // Total in apartment_complexes
  const total = await client.query(`SELECT COUNT(*) as cnt FROM apartment_complexes`);
  console.log(`\nВсего в apartment_complexes: ${total.rows[0].cnt}`);

  await client.end();
}

main().catch(console.error);
