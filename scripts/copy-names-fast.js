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

function fixNamesObject(names) {
  if (!names) return names;
  const fixed = {};
  for (const [lang, arr] of Object.entries(names)) {
    if (Array.isArray(arr)) {
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
  const localClient = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  const railwayClient = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await localClient.connect();
  await railwayClient.connect();

  console.log('=== Быстрое копирование names ===\n');

  // Читаем все из локальной
  const localStreets = await localClient.query(`
    SELECT id, names FROM streets WHERE names IS NOT NULL
  `);
  console.log(`Загружено из локальной базы: ${localStreets.rows.length}`);

  // Подготавливаем данные для batch update
  const batchSize = 500;
  let updated = 0;

  for (let i = 0; i < localStreets.rows.length; i += batchSize) {
    const batch = localStreets.rows.slice(i, i + batchSize);

    // Формируем VALUES для batch update
    const values = batch.map((row, idx) => {
      const fixedNames = fixNamesObject(row.names);
      return `($${idx * 2 + 1}::int, $${idx * 2 + 2}::jsonb)`;
    }).join(',\n');

    const params = [];
    for (const row of batch) {
      params.push(row.id);
      params.push(JSON.stringify(fixNamesObject(row.names)));
    }

    const sql = `
      UPDATE streets AS s
      SET names = v.names
      FROM (VALUES ${values}) AS v(id, names)
      WHERE s.id = v.id
    `;

    await railwayClient.query(sql, params);
    updated += batch.length;

    if (updated % 5000 === 0 || i + batchSize >= localStreets.rows.length) {
      console.log(`Обновлено: ${updated}/${localStreets.rows.length}`);
    }
  }

  console.log(`\nГотово! Обновлено: ${updated}`);

  // Проверка
  console.log('\n=== Проверка ===');
  const check = await railwayClient.query(`
    SELECT id, names->'uk' as names_uk
    FROM streets WHERE id IN (34399, 28305, 31665, 34338)
  `);
  for (const r of check.rows) {
    console.log(`ID ${r.id}: ${JSON.stringify(r.names_uk)}`);
  }

  await localClient.end();
  await railwayClient.end();
}

main().catch(console.error);
