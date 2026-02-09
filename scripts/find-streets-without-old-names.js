const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Одесса и её районы
  const odesaGeoIds = [18271, 19314, 19315, 19316, 19317]; // city + 4 city_districts

  // Найдём улицы в Одессе без старых названий
  const result = await client.query(`
    SELECT s.id, s.name->>'uk' as name_uk, s.names->'uk' as names_uk,
           g.name->>'uk' as district
    FROM streets s
    LEFT JOIN geo g ON s.geo_id = g.id
    WHERE s.geo_id IN (${odesaGeoIds.join(',')})
      AND (
        s.names IS NULL
        OR s.names->'uk' IS NULL
        OR jsonb_array_length(s.names->'uk') <= 1
      )
    ORDER BY s.name->>'uk'
  `);

  console.log(`Улиц в Одессе без старых названий: ${result.rows.length}\n`);

  // Покажем первые 50
  console.log('Первые 50:');
  result.rows.slice(0, 50).forEach((r, i) => {
    console.log(`${i+1}. [${r.id}] ${r.name_uk}`);
  });

  // Сохраним полный список
  const fs = require('fs');
  fs.writeFileSync('output/odesa-streets-no-old-names.json', JSON.stringify(result.rows, null, 2));
  console.log(`\nПолный список сохранён в output/odesa-streets-no-old-names.json`);

  // Статистика по районам
  const byDistrict = {};
  result.rows.forEach(r => {
    const d = r.district || 'unknown';
    byDistrict[d] = (byDistrict[d] || 0) + 1;
  });
  console.log('\nПо районам:');
  Object.entries(byDistrict).forEach(([d, c]) => console.log(`  ${d}: ${c}`));

  await client.end();
}

main().catch(console.error);
