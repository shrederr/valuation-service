const { Client } = require('pg');
const iconv = require('iconv-lite');

// ВАЖНО: Локальная база - ТОЛЬКО ЧТЕНИЕ!
// Запись только в Railway!

function fixEncoding(str) {
  if (!str) return str;
  try {
    const buf = iconv.encode(str, 'win1251');
    return iconv.decode(buf, 'utf8');
  } catch (e) {
    return str;
  }
}

function fixNamesObject(names) {
  if (!names) return names;

  const fixed = {};
  for (const [lang, arr] of Object.entries(names)) {
    if (Array.isArray(arr)) {
      // Для uk и ru применяем fix, для en оставляем как есть
      if (lang === 'uk' || lang === 'ru') {
        fixed[lang] = arr.map(n => fixEncoding(n));
      } else {
        fixed[lang] = arr;
      }
    }
  }
  return fixed;
}

async function main() {
  // Локальная база - ТОЛЬКО ЧТЕНИЕ
  const localClient = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  // Railway - для записи
  const railwayClient = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await localClient.connect();
  await railwayClient.connect();

  console.log('=== Копирование names из локальной базы в Railway ===');
  console.log('Локальная база: ТОЛЬКО ЧТЕНИЕ');
  console.log('Railway: ЗАПИСЬ\n');

  // Читаем все улицы из локальной базы
  const localStreets = await localClient.query(`
    SELECT id, names
    FROM streets
    WHERE names IS NOT NULL
  `);

  console.log(`Найдено улиц в локальной базе: ${localStreets.rows.length}`);

  let updated = 0;
  let errors = 0;
  const batchSize = 100;

  for (let i = 0; i < localStreets.rows.length; i += batchSize) {
    const batch = localStreets.rows.slice(i, i + batchSize);

    for (const row of batch) {
      try {
        const fixedNames = fixNamesObject(row.names);

        await railwayClient.query(
          `UPDATE streets SET names = $1 WHERE id = $2`,
          [JSON.stringify(fixedNames), row.id]
        );
        updated++;
      } catch (err) {
        console.error(`Ошибка для id=${row.id}:`, err.message);
        errors++;
      }
    }

    if ((i + batchSize) % 1000 === 0 || i + batchSize >= localStreets.rows.length) {
      console.log(`Обработано: ${Math.min(i + batchSize, localStreets.rows.length)}/${localStreets.rows.length}`);
    }
  }

  console.log(`\nГотово!`);
  console.log(`Обновлено: ${updated}`);
  console.log(`Ошибок: ${errors}`);

  // Проверяем результат
  console.log('\n=== Проверка результата в Railway ===');
  const check = await railwayClient.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE id IN (34399, 28305, 31665, 34338)
  `);

  for (const row of check.rows) {
    console.log(`\nID ${row.id}:`);
    console.log(`  name_uk: ${row.name_uk}`);
    console.log(`  names_uk: ${JSON.stringify(row.names_uk)}`);
  }

  await localClient.end();
  await railwayClient.end();
}

main().catch(console.error);
