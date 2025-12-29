const fs = require('fs');
const { Client } = require('pg');

function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/жк|жилой комплекс|житловий комплекс|residential complex/gi, '')
    .replace(/кг|км|котеджне|коттеджне|містечко|селище/gi, '')
    .replace(/таунхаус[иі]?|townhouse[s]?|дуплекс[иі]?|duplex[es]?/gi, '')
    .replace(/["«»'']/g, '')
    .replace(/[^\wа-яіїєґ\s]/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });

  await client.connect();
  console.log('Connected to database');

  const data = JSON.parse(fs.readFileSync('D:/analogis/liquidity-define/db/apartment_complexes.json', 'utf8'));
  console.log(`Importing ${data.length} complexes...`);

  let inserted = 0;
  let withPolygon = 0;

  for (const c of data) {
    const nameNormalized = normalizeName(c.nameRu || c.nameUk);

    let polygonValue = null;
    if (c.polygon && c.polygon.length >= 3) {
      const coords = c.polygon.map(p => `${p[0]} ${p[1]}`).join(', ');
      const firstCoord = `${c.polygon[0][0]} ${c.polygon[0][1]}`;
      polygonValue = `POLYGON((${coords}, ${firstCoord}))`;
    }

    try {
      await client.query(`
        INSERT INTO apartment_complexes
        (osm_id, osm_type, name_ru, name_uk, name_en, name_normalized, lat, lng, polygon, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ${polygonValue ? 'ST_GeomFromText($9, 4326)' : 'NULL'}, ${polygonValue ? '$10' : '$9'})
      `, polygonValue
        ? [c.osmId || null, c.osmType || null, c.nameRu, c.nameUk, c.nameEn || null, nameNormalized, c.lat, c.lng, polygonValue, c.source]
        : [c.osmId || null, c.osmType || null, c.nameRu, c.nameUk, c.nameEn || null, nameNormalized, c.lat, c.lng, c.source]
      );
      inserted++;
      if (polygonValue) withPolygon++;
    } catch (err) {
      console.error(`Error inserting ${c.nameRu}:`, err.message);
    }

    if (inserted % 500 === 0) {
      console.log(`  Inserted ${inserted}...`);
    }
  }

  console.log(`\nDone! Inserted ${inserted} complexes (${withPolygon} with polygons)`);

  // Stats
  const stats = await client.query(`
    SELECT source, COUNT(*) as count, COUNT(polygon) as with_polygon
    FROM apartment_complexes GROUP BY source
  `);
  console.log('\nStats by source:');
  stats.rows.forEach(r => console.log(`  ${r.source}: ${r.count} (${r.with_polygon} with polygon)`));

  await client.end();
}

main().catch(console.error);
