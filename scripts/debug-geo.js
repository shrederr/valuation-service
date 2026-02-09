const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Проверим какие geo содержат улицу 34294
  const containing = await client.query(`
    SELECT g.id, g.name->>'uk' as name, g.type, g.lft, g.rgt,
           ST_Contains(g.polygon, ST_Centroid(s.line)) as contains
    FROM geo g, streets s
    WHERE s.id = 34294
      AND g.polygon IS NOT NULL
      AND g.id IN (18263, 18266, 18271, 19316)
    ORDER BY g.lft
  `);

  console.log('Geo и содержат ли они улицу 34294:');
  containing.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type}) lft=${r.lft} contains=${r.contains}`);
  });

  // Проверим координаты улицы
  const street = await client.query(`
    SELECT id, name->>'uk' as name,
           ST_AsText(ST_Centroid(line)) as centroid,
           ST_X(ST_Centroid(line)) as lng,
           ST_Y(ST_Centroid(line)) as lat
    FROM streets
    WHERE id = 34294
  `);
  console.log('\nУлица 34294:');
  console.log(`  ${street.rows[0].name}`);
  console.log(`  Центроид: ${street.rows[0].lat}, ${street.rows[0].lng}`);

  // Найдём самый вложенный geo который содержит эту точку
  const best = await client.query(`
    SELECT g.id, g.name->>'uk' as name, g.type, g.lft
    FROM geo g
    WHERE g.polygon IS NOT NULL
      AND ST_Contains(g.polygon, ST_SetSRID(ST_MakePoint($1, $2), 4326))
    ORDER BY g.lft DESC
    LIMIT 5
  `, [street.rows[0].lng, street.rows[0].lat]);

  console.log('\nGeo содержащие центроид улицы (по lft DESC):');
  best.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type}) lft=${r.lft}`);
  });

  await client.end();
}

main().catch(console.error);
