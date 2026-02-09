const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Построение маппинга старых названий улиц ===\n');

  // Найдём все geo для Одессы
  const odessaGeo = await client.query(`
    SELECT id, name->>'uk' as name_uk
    FROM geo
    WHERE name->>'uk' ILIKE '%одес%'
       OR name->>'ru' ILIKE '%одес%'
  `);
  console.log(`Найдено ${odessaGeo.rows.length} geo для Одессы`);
  const odessaGeoIds = odessaGeo.rows.map(g => g.id);

  // Получим все улицы с несколькими названиями
  const streets = await client.query(`
    SELECT id, name->>'uk' as current_name, names->'uk' as uk_names, geo_id
    FROM streets
    WHERE names IS NOT NULL
      AND jsonb_array_length(names->'uk') > 1
  `);

  console.log(`Всего улиц с несколькими названиями: ${streets.rows.length}`);

  // Построим маппинг: старое_название -> {street_id, current_name, geo_id}
  const renameMap = {};
  let odessaCount = 0;

  for (const street of streets.rows) {
    const ukNames = street.uk_names || [];
    if (ukNames.length < 2) continue;

    const currentName = ukNames[0]; // Первое - актуальное
    const oldNames = ukNames.slice(1); // Остальные - старые

    const isOdessa = odessaGeoIds.includes(street.geo_id);
    if (isOdessa) odessaCount++;

    for (const oldName of oldNames) {
      // Нормализуем для поиска
      const normalizedOld = normalizeStreetName(oldName);

      if (!renameMap[normalizedOld]) {
        renameMap[normalizedOld] = [];
      }

      renameMap[normalizedOld].push({
        streetId: street.id,
        currentName: currentName,
        oldName: oldName,
        geoId: street.geo_id,
        isOdessa: isOdessa
      });
    }
  }

  console.log(`Улиц Одессы с переименованиями: ${odessaCount}`);
  console.log(`Уникальных старых названий в маппинге: ${Object.keys(renameMap).length}`);

  // Отфильтруем только Одессу
  const odessaRenameMap = {};
  for (const [oldName, entries] of Object.entries(renameMap)) {
    const odessaEntries = entries.filter(e => e.isOdessa);
    if (odessaEntries.length > 0) {
      odessaRenameMap[oldName] = odessaEntries;
    }
  }

  console.log(`Старых названий в Одессе: ${Object.keys(odessaRenameMap).length}`);

  // Сохраним
  const outputDir = path.join(__dirname, '../output');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  fs.writeFileSync(
    path.join(outputDir, 'odessa-rename-map.json'),
    JSON.stringify(odessaRenameMap, null, 2)
  );

  fs.writeFileSync(
    path.join(outputDir, 'all-rename-map.json'),
    JSON.stringify(renameMap, null, 2)
  );

  // Примеры для Одессы
  console.log('\n--- Примеры переименований в Одессе ---');
  const examples = Object.entries(odessaRenameMap).slice(0, 30);
  examples.forEach(([oldName, entries], i) => {
    const e = entries[0];
    console.log(`${i+1}. "${oldName}" → "${e.currentName}"`);
  });

  // Проверим конкретно Говорова и Дворянська
  console.log('\n--- Поиск Говорова ---');
  for (const [oldName, entries] of Object.entries(odessaRenameMap)) {
    if (oldName.includes('говоров')) {
      console.log(`"${oldName}" → "${entries[0].currentName}"`);
    }
  }

  console.log('\n--- Поиск Дворянська ---');
  for (const [oldName, entries] of Object.entries(odessaRenameMap)) {
    if (oldName.includes('дворян')) {
      console.log(`"${oldName}" → "${entries[0].currentName}"`);
    }
  }

  await client.end();
  console.log('\nФайлы сохранены в output/');
}

function normalizeStreetName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
    .replace(/\s+(вулиця|улица|проспект|провулок|переулок|бульвар|площа|площадь|набережна|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)$/gi, '')
    .replace(/[«»""''`']/g, '')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

main().catch(console.error);
