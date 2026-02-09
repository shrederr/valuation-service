const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Поиск улиц с несколькими названиями в names ===\n');

  // Найдём geo_id для Одессы
  const odessaGeo = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM geo
    WHERE name->>'uk' ILIKE '%одес%' OR name->>'ru' ILIKE '%одес%'
    LIMIT 10
  `);
  console.log('Geo Одесса:');
  odessaGeo.rows.forEach(g => console.log(`  [${g.id}] ${g.name_uk} / ${g.name_ru}`));

  const odessaGeoIds = odessaGeo.rows.map(g => g.id);

  // Улицы где в names->uk больше одного элемента
  const multiNames = await client.query(`
    SELECT id, name->>'uk' as name_uk, names, geo_id
    FROM streets
    WHERE names IS NOT NULL
      AND jsonb_array_length(names->'uk') > 1
    ORDER BY id
    LIMIT 50
  `);

  console.log(`\n--- Улицы с несколькими названиями в uk (${multiNames.rows.length}) ---`);
  multiNames.rows.forEach((s, i) => {
    const ukNames = s.names?.uk || [];
    console.log(`${i+1}. [${s.id}] ${s.name_uk} | geo_id: ${s.geo_id}`);
    console.log(`   uk names: ${JSON.stringify(ukNames)}`);
  });

  // Проверим конкретно для Одессы (geo_id из списка)
  if (odessaGeoIds.length > 0) {
    const odessaMulti = await client.query(`
      SELECT id, name->>'uk' as name_uk, names, geo_id
      FROM streets
      WHERE geo_id = ANY($1)
        AND names IS NOT NULL
        AND jsonb_array_length(names->'uk') > 1
      ORDER BY id
    `, [odessaGeoIds]);

    console.log(`\n--- Улицы Одессы с несколькими названиями (${odessaMulti.rows.length}) ---`);
    odessaMulti.rows.forEach((s, i) => {
      const ukNames = s.names?.uk || [];
      console.log(`${i+1}. [${s.id}] ${s.name_uk}`);
      console.log(`   uk names: ${JSON.stringify(ukNames)}`);
    });
  }

  // Также проверим всего сколько улиц имеют >1 название
  const countMulti = await client.query(`
    SELECT COUNT(*) as count
    FROM streets
    WHERE names IS NOT NULL
      AND jsonb_array_length(names->'uk') > 1
  `);
  console.log(`\nВсего улиц с >1 названием в uk: ${countMulti.rows[0].count}`);

  // Статистика по количеству названий
  const stats = await client.query(`
    SELECT jsonb_array_length(names->'uk') as names_count, COUNT(*) as streets_count
    FROM streets
    WHERE names IS NOT NULL
    GROUP BY jsonb_array_length(names->'uk')
    ORDER BY names_count
  `);
  console.log('\n--- Распределение по количеству названий ---');
  stats.rows.forEach(s => console.log(`  ${s.names_count} названий: ${s.streets_count} улиц`));

  await client.end();
}

main().catch(console.error);
