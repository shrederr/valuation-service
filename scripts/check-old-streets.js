/**
 * Проверка старых названий улиц в базе
 */

const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Проверка старых названий улиц ===\n');

  // 1. Сколько улиц с is_old_street = true
  const oldStreetsCount = await client.query(`
    SELECT COUNT(*) as count FROM streets WHERE is_old_street = true
  `);
  console.log(`Улиц с is_old_street=true: ${oldStreetsCount.rows[0].count}`);

  // 2. Сколько улиц с заполненным names
  const withNamesCount = await client.query(`
    SELECT COUNT(*) as count FROM streets WHERE names IS NOT NULL AND names != 'null'
  `);
  console.log(`Улиц с заполненным names: ${withNamesCount.rows[0].count}`);

  // 3. Сколько улиц с заполненным alias
  const withAliasCount = await client.query(`
    SELECT COUNT(*) as count FROM streets WHERE alias IS NOT NULL AND alias != ''
  `);
  console.log(`Улиц с заполненным alias: ${withAliasCount.rows[0].count}`);

  // 4. Примеры улиц с is_old_street = true
  const oldStreets = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, alias, names, geo_id
    FROM streets
    WHERE is_old_street = true
    LIMIT 20
  `);
  console.log('\n--- Примеры старых улиц (is_old_street=true) ---');
  oldStreets.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru}`);
    if (s.alias) console.log(`   alias: ${s.alias}`);
    if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  // 5. Примеры улиц с заполненным names
  const withNames = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, alias, names, geo_id
    FROM streets
    WHERE names IS NOT NULL AND names != 'null' AND jsonb_typeof(names) = 'array' AND jsonb_array_length(names) > 0
    LIMIT 20
  `);
  console.log('\n--- Примеры улиц с заполненным names ---');
  withNames.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru}`);
    console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  // 6. Примеры улиц с alias
  const withAlias = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, alias, geo_id
    FROM streets
    WHERE alias IS NOT NULL AND alias != ''
    LIMIT 20
  `);
  console.log('\n--- Примеры улиц с alias ---');
  withAlias.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru}`);
    console.log(`   alias: ${s.alias}`);
  });

  // 7. Проверим конкретно Одессу - улицы с is_old_street
  const odessaOld = await client.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru, s.alias, s.names, g.name->>'uk' as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE s.is_old_street = true
    AND g.name->>'uk' ILIKE '%одес%'
    LIMIT 30
  `);
  console.log('\n--- Старые улицы Одессы ---');
  console.log(`Найдено: ${odessaOld.rows.length}`);
  odessaOld.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} (${s.geo_name})`);
    if (s.alias) console.log(`   alias: ${s.alias}`);
    if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  // 8. Поищем конкретно "Говорова" и "Добровольців"
  console.log('\n--- Поиск Говорова/Добровольців ---');
  const govorovaSearch = await client.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru, s.alias, s.names, s.is_old_street, g.name->>'uk' as geo_name
    FROM streets s
    JOIN geo g ON s.geo_id = g.id
    WHERE (s.name->>'uk' ILIKE '%говоров%' OR s.name->>'ru' ILIKE '%говоров%'
           OR s.name->>'uk' ILIKE '%добровол%' OR s.name->>'ru' ILIKE '%добровол%')
    AND g.name->>'uk' ILIKE '%одес%'
  `);
  govorovaSearch.rows.forEach((s, i) => {
    console.log(`${i+1}. [${s.id}] ${s.name_uk || s.name_ru} | is_old: ${s.is_old_street} | geo: ${s.geo_name}`);
    if (s.alias) console.log(`   alias: ${s.alias}`);
    if (s.names) console.log(`   names: ${JSON.stringify(s.names)}`);
  });

  await client.end();
}

main().catch(console.error);
