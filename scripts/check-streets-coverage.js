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

  // Streets per region
  const r = await c.query(`
    SELECT
      g.name->>'uk' as region,
      g.alias,
      COUNT(DISTINCT s.id) as streets_count,
      COUNT(DISTINCT s.id) FILTER (WHERE s.line IS NOT NULL) as with_geometry
    FROM geo g
    LEFT JOIN geo child ON child.lft > g.lft AND child.rgt < g.rgt
    LEFT JOIN streets s ON s.geo_id = child.id
    WHERE g.type = 'region'
    GROUP BY g.id, g.name, g.alias
    ORDER BY g.name->>'uk'
  `);

  console.log('Покрытие улицами по регионам:');
  console.log('='.repeat(70));

  let total = 0;
  let totalWithGeo = 0;

  r.rows.forEach(row => {
    const streets = parseInt(row.streets_count) || 0;
    const withGeo = parseInt(row.with_geometry) || 0;
    total += streets;
    totalWithGeo += withGeo;

    const status = streets === 0 ? '❌ НЕТ УЛИЦ' :
                   withGeo < streets ? '⚠️ нет геометрии' : '✅';

    console.log(`${row.region.padEnd(30)} ${streets.toString().padStart(6)} улиц  ${status}`);
  });

  console.log('='.repeat(70));
  console.log(`ВСЕГО: ${total} улиц, ${totalWithGeo} с геометрией`);

  // Check settlements without streets
  const noStreets = await c.query(`
    SELECT
      g.name->>'uk' as region,
      COUNT(*) as settlements_without_streets
    FROM geo g
    INNER JOIN geo child ON child.lft > g.lft AND child.rgt < g.rgt
    WHERE g.type = 'region'
      AND child.type IN ('city', 'village')
      AND child.polygon IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM streets s WHERE s.geo_id = child.id)
    GROUP BY g.id, g.name
    ORDER BY COUNT(*) DESC
  `);

  console.log('\nНаселённые пункты БЕЗ улиц:');
  noStreets.rows.forEach(row => {
    console.log(`  ${row.region}: ${row.settlements_without_streets}`);
  });

  await c.end();
}

main().catch(console.error);
