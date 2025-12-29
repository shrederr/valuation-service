const { DataSource } = require('typeorm');
require('dotenv').config();

const ds = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
});

async function main() {
  await ds.initialize();
  console.log('Connected\n');

  // Stats
  const stats = await ds.query(`
    SELECT COUNT(*) as total, COUNT(polygon) as with_polygon FROM apartment_complexes
  `);
  console.log('Stats:', stats[0]);

  // Test point-in-polygon for a known complex
  // ЖК Аврора coordinates: 46.4269, 30.7488
  const testLat = 46.4269;
  const testLng = 30.7488;

  const result = await ds.query(`
    SELECT id, name_ru,
           ST_Distance(
             ST_Transform(polygon, 3857),
             ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 3857)
           ) as distance_m
    FROM apartment_complexes
    WHERE polygon IS NOT NULL
      AND ST_Contains(polygon, ST_SetSRID(ST_MakePoint($1, $2), 4326))
    LIMIT 1
  `, [testLng, testLat]);

  if (result.length > 0) {
    console.log('\nPoint-in-polygon match:', result[0].name_ru);
  } else {
    console.log('\nNo point-in-polygon match, trying nearest...');
    const nearest = await ds.query(`
      SELECT id, name_ru,
             ST_Distance(
               ST_Transform(ST_Centroid(polygon), 3857),
               ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 3857)
             ) as distance_m
      FROM apartment_complexes
      WHERE polygon IS NOT NULL
      ORDER BY ST_Distance(
        ST_Centroid(polygon),
        ST_SetSRID(ST_MakePoint($1, $2), 4326)
      )
      LIMIT 3
    `, [testLng, testLat]);
    console.log('Nearest complexes:');
    nearest.forEach(n => console.log(`  - ${n.name_ru}: ${Math.round(n.distance_m)}m`));
  }

  // Also test with a known listing from the dump
  const sample = await ds.query(`
    SELECT id, lat, lng, title FROM unified_listings
    WHERE lat IS NOT NULL AND complex_id IS NULL
    LIMIT 1
  `).catch(() => []);

  if (sample.length > 0) {
    console.log('\nTest listing:', sample[0].title?.substring(0, 50));
    const match = await ds.query(`
      SELECT id, name_ru FROM apartment_complexes
      WHERE polygon IS NOT NULL
        AND ST_Contains(polygon, ST_SetSRID(ST_MakePoint($1, $2), 4326))
      LIMIT 1
    `, [sample[0].lng, sample[0].lat]);
    if (match.length > 0) {
      console.log('Found complex:', match[0].name_ru);
    } else {
      console.log('No polygon match for this listing');
    }
  }

  await ds.destroy();
}

main().catch(console.error);
