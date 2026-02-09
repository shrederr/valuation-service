/**
 * Проверка старых улиц в vector-api базе
 */

const { Client } = require('pg');

async function main() {
  // Подключаемся к vector-api базе напрямую
  const client = new Client({
    host: 'localhost',
    port: 54321,
    database: 'vector',
    user: 'postgres',
    password: 'postgres'
  });

  try {
    await client.connect();
    console.log('=== Vector-API DB - проверка старых улиц ===\n');

    // Структура таблицы
    const cols = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'streets'
      ORDER BY ordinal_position
    `);
    console.log('Колонки streets:');
    cols.rows.forEach(c => console.log(`  - ${c.column_name} (${c.data_type})`));

    // Сколько улиц is_old_street
    const oldCount = await client.query(`
      SELECT
        COUNT(*) FILTER (WHERE is_old_street = true) as old_streets,
        COUNT(*) FILTER (WHERE is_old_street = false OR is_old_street IS NULL) as current_streets,
        COUNT(*) as total
      FROM streets
      WHERE deleted_at IS NULL
    `);
    console.log('\n--- Статистика ---');
    console.log(`Старых улиц (is_old_street=true): ${oldCount.rows[0].old_streets}`);
    console.log(`Актуальных улиц: ${oldCount.rows[0].current_streets}`);
    console.log(`Всего: ${oldCount.rows[0].total}`);

    // Примеры старых улиц
    const oldStreets = await client.query(`
      SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru,
             s.names, g.name->>'uk' as geo_name
      FROM streets s
      LEFT JOIN geo g ON s.geo_id = g.id
      WHERE s.is_old_street = true AND s.deleted_at IS NULL
      LIMIT 30
    `);
    console.log('\n--- Примеры старых улиц (is_old_street=true) ---');
    oldStreets.rows.forEach((s, i) => {
      console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} | geo: ${s.geo_name}`);
      if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
    });

    // Старые улицы Одессы
    const odessaOld = await client.query(`
      SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru,
             s.names, g.name->>'uk' as geo_name
      FROM streets s
      LEFT JOIN geo g ON s.geo_id = g.id
      WHERE s.is_old_street = true
        AND s.deleted_at IS NULL
        AND (g.name->>'uk' ILIKE '%одес%' OR g.name->>'ru' ILIKE '%одес%')
      ORDER BY s.name->>'uk'
    `);
    console.log('\n--- Старые улицы Одессы ---');
    console.log(`Найдено: ${odessaOld.rows.length}`);
    odessaOld.rows.forEach((s, i) => {
      console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru}`);
      if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
    });

    // Проверим names формат
    const withNames = await client.query(`
      SELECT s.id, s.name->>'uk' as name_uk, s.names
      FROM streets s
      WHERE s.names IS NOT NULL
        AND s.names::text != 'null'
        AND s.deleted_at IS NULL
      LIMIT 10
    `);
    console.log('\n--- Примеры с заполненным names ---');
    withNames.rows.forEach((s, i) => {
      console.log(`${i+1}. [${s.id}] ${s.name_uk}`);
      console.log(`   names: ${JSON.stringify(s.names)}`);
    });

    // Поиск Говорова в Одессе
    console.log('\n--- Поиск Говорова/Добровольців в Одессе ---');
    const govorova = await client.query(`
      SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru,
             s.is_old_street, s.names, g.name->>'uk' as geo_name
      FROM streets s
      LEFT JOIN geo g ON s.geo_id = g.id
      WHERE s.deleted_at IS NULL
        AND (g.name->>'uk' ILIKE '%одес%' OR g.name->>'ru' ILIKE '%одес%')
        AND (s.name->>'uk' ILIKE '%говоров%' OR s.name->>'ru' ILIKE '%говоров%'
             OR s.name->>'uk' ILIKE '%добровол%' OR s.name->>'ru' ILIKE '%добровол%')
    `);
    govorova.rows.forEach((s, i) => {
      console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} | is_old: ${s.is_old_street} | geo: ${s.geo_name}`);
      if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
    });

  } catch (err) {
    console.error('Ошибка:', err.message);

    // Попробуем другой порт
    console.log('\nПробуем порт 5432...');
    const client2 = new Client({
      host: 'localhost',
      port: 5432,
      database: 'vector',
      user: 'postgres',
      password: 'postgres'
    });

    try {
      await client2.connect();
      console.log('Подключились к порту 5432');

      const cols = await client2.query(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'streets' LIMIT 5
      `);
      console.log('Колонки:', cols.rows.map(c => c.column_name).join(', '));

      await client2.end();
    } catch (err2) {
      console.error('Ошибка порт 5432:', err2.message);
    }
  }

  await client.end().catch(() => {});
}

main().catch(console.error);
