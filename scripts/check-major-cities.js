const { Client } = require('pg');

async function main() {
  const c = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });

  await c.connect();

  // Check all cities by type
  const cities = await c.query(`
    SELECT id, name->>'uk' as name, type, alias,
           (polygon IS NOT NULL) as has_polygon
    FROM geo
    WHERE type = 'city'
    ORDER BY name->>'uk'
  `);

  console.log('Все города (type=city) в базе:');
  console.log('='.repeat(70));
  cities.rows.forEach(r => {
    const poly = r.has_polygon ? '✅' : '❌';
    console.log(`${r.id.toString().padEnd(6)} ${r.name.padEnd(30)} poly:${poly}`);
  });
  console.log(`\nВсего городов: ${cities.rows.length}`);

  // Check Odesa specifically
  console.log('\n--- Одеса ---');
  const odesa = await c.query(`
    SELECT id, name, type, alias, (polygon IS NOT NULL) as has_polygon
    FROM geo WHERE id = 9
  `);
  console.log(odesa.rows[0]);

  // Check streets for Odesa
  const odesaStreets = await c.query(`
    SELECT COUNT(*) as cnt FROM streets WHERE geo_id = 9
  `);
  console.log('Streets in Odesa (geo_id=9):', odesaStreets.rows[0].cnt);

  // Check if there are streets linked to any Odesa district
  const odesaDistricts = await c.query(`
    SELECT g.id, g.name->>'uk' as name, g.type, COUNT(s.id) as streets
    FROM geo g
    LEFT JOIN streets s ON s.geo_id = g.id
    WHERE g.name->>'uk' ILIKE '%одес%'
    GROUP BY g.id
    ORDER BY g.type
  `);
  console.log('\nВсе гео с "Одес":');
  odesaDistricts.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type}) - ${r.streets} улиц`);
  });

  await c.end();
}

main().catch(console.error);
