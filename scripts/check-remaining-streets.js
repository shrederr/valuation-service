const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  const odesaGeoIds = [18271, 19314, 19315, 19316, 19317];

  // Улицы без старых названий
  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE geo_id IN (${odesaGeoIds.join(',')})
      AND (names IS NULL OR names->'uk' IS NULL OR jsonb_array_length(names->'uk') <= 1)
    ORDER BY name->>'uk'
  `);

  console.log(`Улиц без старых названий: ${result.rows.length}\n`);

  // Группируем по типу названия
  const byType = {
    numbered: [],      // 1-й, 2-й, 10-та лінія и т.д.
    station: [],       // станції Фонтану, Люстдорфської дороги
    regular: [],       // обычные улицы
  };

  for (const r of result.rows) {
    const name = r.name_uk || '';
    if (/^\d|[0-9]-[йаяі]|лінія|станц/i.test(name)) {
      byType.numbered.push(name);
    } else if (/станц|Фонтан/i.test(name)) {
      byType.station.push(name);
    } else {
      byType.regular.push(name);
    }
  }

  console.log(`Нумерованные (линии, станции): ${byType.numbered.length}`);
  console.log(`Обычные улицы: ${byType.regular.length}\n`);

  // Показываем обычные улицы (которые могли быть переименованы)
  console.log('=== Обычные улицы без старых названий (первые 100) ===\n');
  byType.regular.slice(0, 100).forEach((name, i) => {
    console.log(`${i+1}. ${name}`);
  });

  // Сохраним в файл
  const fs = require('fs');
  const outputDir = 'D:/analogis/liquidity-define/output';
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  fs.writeFileSync(`${outputDir}/streets-without-old-names.json`, JSON.stringify({
    total: result.rows.length,
    numbered: byType.numbered,
    regular: byType.regular
  }, null, 2));

  console.log(`\nСохранено в output/streets-without-old-names.json`);

  await client.end();
}

main().catch(console.error);
