const { Client } = require('pg');
const fs = require('fs');

// ТОЛЬКО ЧТЕНИЕ! Никаких UPDATE/INSERT/DELETE!

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  try {
    await client.connect();
    console.log('=== Проверка локальной базы (ТОЛЬКО ЧТЕНИЕ) ===\n');

    // Проверим есть ли таблица streets
    const tables = await client.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'streets'
    `);

    if (tables.rows.length === 0) {
      console.log('Таблица streets НЕ НАЙДЕНА');
      await client.end();
      return;
    }

    console.log('Таблица streets найдена');

    // Количество улиц
    const count = await client.query('SELECT COUNT(*) as cnt FROM streets');
    console.log('Всего улиц:', count.rows[0].cnt);

    // Проверим структуру
    const cols = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'streets'
      ORDER BY ordinal_position
    `);
    console.log('\nКолонки:', cols.rows.map(c => c.column_name).join(', '));

    // Примеры данных - проверим кодировку
    const examples = await client.query(`
      SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
      FROM streets
      WHERE id IN (34399, 28305, 31665, 34338)
    `);

    // Сохраним в файл для проверки кодировки
    fs.writeFileSync('output/local-streets.json', JSON.stringify(examples.rows, null, 2));
    console.log('\nПримеры сохранены в output/local-streets.json');

    // Также проверим статистику кодировки
    const stats = await client.query(`
      SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE names->'uk'->>0 ~ '[іїєґ]') as has_ukr,
        COUNT(*) FILTER (WHERE names->'uk'->>0 ~ '�') as broken
      FROM streets
      WHERE names IS NOT NULL AND jsonb_array_length(names->'uk') > 0
    `);
    console.log('\nСтатистика кодировки:');
    console.log('Всего с names:', stats.rows[0].total);
    console.log('С укр. буквами:', stats.rows[0].has_ukr);
    console.log('С знаками ?:', stats.rows[0].broken);

    await client.end();
  } catch (err) {
    console.error('Ошибка:', err.message);
  }
}

main();
