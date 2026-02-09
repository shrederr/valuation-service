const { Client } = require('pg');

async function checkStreet() {
  const client = new Client({
    host: 'localhost',
    port: 5438,
    database: 'aggregator_db',
    user: 'postgres',
    password: 'admin'
  });

  await client.connect();

  // Поиск по разным вариантам написания
  const queries = [
    'Добровольців',
    'добровольців',
    'Добровольцев',
    'добровольцев',
    'Добровольц'
  ];

  console.log('=== Поиск улицы "Добровольців" ===\n');

  for (const q of queries) {
    const result = await client.query(`
      SELECT id, name, geo_id
      FROM streets
      WHERE LOWER(name) LIKE '%' || LOWER($1) || '%'
      LIMIT 5
    `, [q]);

    if (result.rows.length > 0) {
      console.log('Поиск по:', q);
      console.log(result.rows);
      console.log('');
    }
  }

  // Также проверим все улицы в Одессе с похожими названиями
  const odessaStreets = await client.query(`
    SELECT s.id, s.name, g.name as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE g.name ILIKE '%одес%'
    AND s.name ILIKE '%добровол%'
    LIMIT 10
  `);

  console.log('Улицы Добровол* в Одессе:');
  console.log(odessaStreets.rows);

  // Проверим также "Ицхака Рабина" (то что GPT-4o неправильно сказал)
  const rabinaStreets = await client.query(`
    SELECT s.id, s.name, g.name as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE g.name ILIKE '%одес%'
    AND (s.name ILIKE '%рабін%' OR s.name ILIKE '%рабин%')
    LIMIT 10
  `);

  console.log('\nУлицы Рабіна/Рабина в Одессе:');
  console.log(rabinaStreets.rows);

  await client.end();
}

checkStreet().catch(console.error);
