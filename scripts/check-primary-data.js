const { Client } = require('pg');

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 54325,
    database: 'vector',
    user: 'vector',
    password: 'vector',
  });
  await client.connect();

  // Проверяем есть ли поле primary_data
  console.log('=== ПРОВЕРКА ПОЛЯ primary_data ===\n');
  const columns = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'exported_properties'
      AND column_name LIKE '%primary%' OR column_name LIKE '%raw%' OR column_name LIKE '%original%'
  `);

  if (columns.rows.length === 0) {
    console.log('Поле primary_data НЕ найдено в exported_properties');
    console.log('\nВсе колонки таблицы:');
    const allCols = await client.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_name = 'exported_properties'
    `);
    console.log(allCols.rows.map(r => r.column_name).join(', '));
  } else {
    console.log('Найдены поля:', columns.rows);
  }

  // Проверяем другие таблицы с primary_data
  console.log('\n=== ТАБЛИЦЫ С ПОЛЕМ primary_data ===\n');
  const tables = await client.query(`
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE column_name = 'primary_data'
  `);

  for (const t of tables.rows) {
    console.log(`${t.table_name}.${t.column_name} (${t.data_type})`);
  }

  // Если есть - покажем пример
  if (tables.rows.length > 0) {
    const tableName = tables.rows[0].table_name;
    console.log(`\n=== ПРИМЕР ИЗ ${tableName} (realtor_ua) ===\n`);

    const example = await client.query(`
      SELECT id, primary_data
      FROM ${tableName}
      WHERE primary_data IS NOT NULL
      LIMIT 1
    `);

    if (example.rows.length > 0) {
      console.log('ID:', example.rows[0].id);
      console.log('primary_data:', JSON.stringify(example.rows[0].primary_data, null, 2));
    }
  }

  // Список всех таблиц
  console.log('\n=== ВСЕ ТАБЛИЦЫ В БД ===\n');
  const allTables = await client.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  console.log(allTables.rows.map(r => r.table_name).join('\n'));

  await client.end();
}
main().catch(console.error);
