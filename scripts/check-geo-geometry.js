const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Проверим структуру geo
  const cols = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'geo'
    ORDER BY ordinal_position
  `);
  console.log('Колонки geo:');
  cols.rows.forEach(c => console.log(`  ${c.column_name}: ${c.data_type}`));

  // Проверим есть ли полигоны
  const hasGeom = await client.query(`
    SELECT COUNT(*) as total,
           COUNT(polygon) as with_polygon
    FROM geo
  `);
  console.log('\nГеометрия:');
  console.log(`  Всего geo: ${hasGeom.rows[0].total}`);
  console.log(`  С polygon: ${hasGeom.rows[0].with_polygon}`);

  // Проверим для Одессы - есть ли полигоны у районов
  const odesaDistricts = await client.query(`
    SELECT id, name->>'uk' as name, type,
           CASE WHEN polygon IS NOT NULL THEN 'YES' ELSE 'NO' END as has_polygon
    FROM geo
    WHERE parent_id = 18271 OR id = 18271
    ORDER BY id
  `);
  console.log('\nОдесса и её районы:');
  odesaDistricts.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type}) - polygon: ${r.has_polygon}`);
  });

  // Тест - найдём geo для одной улицы по геометрии
  const testStreet = await client.query(`
    SELECT s.id, s.name->>'uk' as street_name, s.geo_id,
           g.name->>'uk' as current_geo_name
    FROM streets s
    LEFT JOIN geo g ON s.geo_id = g.id
    WHERE s.id = 34294
  `);
  console.log('\nТестовая улица (34294):');
  console.log(`  ${testStreet.rows[0].street_name}`);
  console.log(`  Текущий geo_id: ${testStreet.rows[0].geo_id}`);

  // Найдём все geo которые содержат эту улицу
  const containingGeo = await client.query(`
    SELECT g.id, g.name->>'uk' as name, g.type, g.parent_id
    FROM geo g, streets s
    WHERE s.id = 34294
      AND g.polygon IS NOT NULL
      AND ST_Contains(g.polygon, ST_Centroid(s.line))
    ORDER BY g.lft DESC
    LIMIT 10
  `);
  console.log('\nGeo содержащие улицу (от самого вложенного):');
  containingGeo.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type})`);
  });

  await client.end();
}

main().catch(console.error);
