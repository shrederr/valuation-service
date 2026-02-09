/**
 * Проверка структуры streets и старых названий
 */

const { Client } = require('pg');

async function main() {
  // Сначала проверим структуру в Railway (aggregator copy)
  const railwayClient = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await railwayClient.connect();
  console.log('=== Railway DB (valuation) - структура streets ===\n');

  const railwayCols = await railwayClient.query(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'streets'
    ORDER BY ordinal_position
  `);
  console.log('Колонки:');
  railwayCols.rows.forEach(c => console.log(`  - ${c.column_name} (${c.data_type})`));

  // Примеры улиц с names и alias
  const examples = await railwayClient.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru,
           alias, geo_id
    FROM streets
    WHERE alias IS NOT NULL AND alias != ''
    LIMIT 10
  `);
  console.log('\n--- Примеры с alias ---');
  examples.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru}`);
    console.log(`   alias: ${s.alias}`);
  });

  // Поиск Говорова/Добровольців
  console.log('\n--- Поиск Говорова/Добровольців ---');
  const govorovaSearch = await railwayClient.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru, s.alias, g.name->>'uk' as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE (s.name->>'uk' ILIKE '%говоров%' OR s.name->>'ru' ILIKE '%говоров%'
           OR s.name->>'uk' ILIKE '%добровол%' OR s.name->>'ru' ILIKE '%добровол%'
           OR s.alias ILIKE '%говоров%' OR s.alias ILIKE '%добровол%')
  `);
  console.log(`Найдено: ${govorovaSearch.rows.length}`);
  govorovaSearch.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} | geo: ${s.geo_name}`);
    if (s.alias) console.log(`   alias: ${s.alias}`);
  });

  // Поиск Дворянська/Змієнка
  console.log('\n--- Поиск Дворянська/Змієнка ---');
  const dvoryankaSearch = await railwayClient.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru, s.alias, g.name->>'uk' as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE (s.name->>'uk' ILIKE '%дворян%' OR s.name->>'ru' ILIKE '%дворян%'
           OR s.name->>'uk' ILIKE '%змієнк%' OR s.name->>'ru' ILIKE '%змиенк%'
           OR s.alias ILIKE '%дворян%' OR s.alias ILIKE '%змієнк%')
  `);
  console.log(`Найдено: ${dvoryankaSearch.rows.length}`);
  dvoryankaSearch.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} | geo: ${s.geo_name}`);
    if (s.alias) console.log(`   alias: ${s.alias}`);
  });

  // Проверим сколько всего улиц с alias в Одессе
  const odessaAliasCount = await railwayClient.query(`
    SELECT COUNT(*) as count
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE g.name->>'uk' ILIKE '%одес%'
    AND s.alias IS NOT NULL AND s.alias != ''
  `);
  console.log(`\nУлиц в Одессе с alias: ${odessaAliasCount.rows[0].count}`);

  // Все улицы Одессы с alias
  const odessaAlias = await railwayClient.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.alias
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE g.name->>'uk' ILIKE '%одес%'
    AND s.alias IS NOT NULL AND s.alias != ''
    ORDER BY s.name->>'uk'
    LIMIT 50
  `);
  console.log('\n--- Улицы Одессы с alias (первые 50) ---');
  odessaAlias.rows.forEach((s, i) => {
    console.log(`${i+1}. ${s.name_uk} | alias: ${s.alias}`);
  });

  await railwayClient.end();
}

main().catch(console.error);
