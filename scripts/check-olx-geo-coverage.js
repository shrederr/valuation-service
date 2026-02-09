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

  // OLX без street_id - в каких гео находятся?
  const r = await c.query(`
    SELECT
      g.name->>'uk' as geo_name,
      g.type,
      COUNT(*) as olx_count,
      COUNT(*) FILTER (WHERE u.street_id IS NOT NULL) as with_street,
      COUNT(DISTINCT s.id) as streets_in_geo
    FROM unified_listings u
    LEFT JOIN geo g ON g.id = u.geo_id
    LEFT JOIN streets s ON s.geo_id = g.id
    WHERE u.realty_platform = 'olx'
    GROUP BY g.id, g.name, g.type
    HAVING COUNT(*) > 100
    ORDER BY COUNT(*) DESC
    LIMIT 30
  `);

  console.log('Топ-30 гео с OLX объявлениями:');
  console.log('='.repeat(90));
  console.log('Локация'.padEnd(35) + 'Тип'.padEnd(15) + 'OLX'.padStart(8) + 'С улицей'.padStart(10) + '%'.padStart(6) + 'Улиц в гео'.padStart(12));
  console.log('='.repeat(90));

  r.rows.forEach(row => {
    const olx = parseInt(row.olx_count);
    const withStreet = parseInt(row.with_street);
    const pct = olx > 0 ? Math.round(withStreet / olx * 100) : 0;
    const streetsInGeo = parseInt(row.streets_in_geo) || 0;

    console.log(
      (row.geo_name || 'NULL').substring(0, 34).padEnd(35) +
      (row.type || '-').padEnd(15) +
      olx.toString().padStart(8) +
      withStreet.toString().padStart(10) +
      (pct + '%').padStart(6) +
      streetsInGeo.toString().padStart(12)
    );
  });

  // Проверим OLX с координатами но без street_id - есть ли улицы рядом?
  const noStreetWithCoords = await c.query(`
    SELECT COUNT(*) as cnt
    FROM unified_listings u
    WHERE u.realty_platform = 'olx'
      AND u.street_id IS NULL
      AND u.lat IS NOT NULL
      AND u.lng IS NOT NULL
  `);

  console.log(`\nOLX без street_id но с координатами: ${noStreetWithCoords.rows[0].cnt}`);

  // Сколько из них могли бы получить street_id (улица в 500м)?
  const couldMatch = await c.query(`
    SELECT COUNT(*) as cnt
    FROM unified_listings u
    WHERE u.realty_platform = 'olx'
      AND u.street_id IS NULL
      AND u.lat IS NOT NULL
      AND u.lng IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM streets s
        WHERE s.line IS NOT NULL
        AND ST_DWithin(
          s.line::geography,
          ST_SetSRID(ST_Point(u.lng, u.lat), 4326)::geography,
          500
        )
      )
    LIMIT 10000
  `);

  console.log(`Из них с улицей в радиусе 500м (выборка до 10к): ${couldMatch.rows[0].cnt}`);

  await c.end();
}

main().catch(console.error);
