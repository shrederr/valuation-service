const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== АНАЛІЗ КООРДИНАТ OLX ===\n');

  // 1. Check coordinate ranges for OLX
  const coordRange = await client.query(`
    SELECT
      MIN(lat) as min_lat, MAX(lat) as max_lat,
      MIN(lng) as min_lng, MAX(lng) as max_lng,
      AVG(lat) as avg_lat, AVG(lng) as avg_lng
    FROM unified_listings
    WHERE realty_platform = 'olx' AND lat IS NOT NULL
  `);
  console.log('1. Діапазон координат OLX:');
  console.log(`   Lat: ${coordRange.rows[0].min_lat} - ${coordRange.rows[0].max_lat} (avg: ${parseFloat(coordRange.rows[0].avg_lat).toFixed(4)})`);
  console.log(`   Lng: ${coordRange.rows[0].min_lng} - ${coordRange.rows[0].max_lng} (avg: ${parseFloat(coordRange.rows[0].avg_lng).toFixed(4)})`);

  // 2. Check if OLX coords fall within ANY geo polygon
  const inPolygon = await client.query(`
    SELECT COUNT(*) as cnt
    FROM unified_listings ul
    WHERE ul.realty_platform = 'olx'
      AND ul.lat IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM geo g
        WHERE g.polygon IS NOT NULL
          AND ST_Contains(g.polygon, ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326))
      )
  `);
  console.log(`\n2. OLX в будь-якому полігоні geo: ${inPolygon.rows[0].cnt}`);

  // 3. Distribution by region (from pathName)
  const byRegion = await client.query(`
    SELECT
      SPLIT_PART(primary_data->'location'->>'pathName', ',', 1) as region,
      COUNT(*) as cnt
    FROM unified_listings
    WHERE realty_platform = 'olx' AND primary_data->'location'->>'pathName' IS NOT NULL
    GROUP BY SPLIT_PART(primary_data->'location'->>'pathName', ',', 1)
    ORDER BY cnt DESC
    LIMIT 15
  `);
  console.log('\n3. OLX по областях (з pathName):');
  for (const row of byRegion.rows) {
    console.log(`   ${row.region}: ${row.cnt}`);
  }

  // 4. Check our geo polygons coverage
  const geoPolygons = await client.query(`
    SELECT
      g.type,
      g.name->>'uk' as name_uk,
      COUNT(*) as polygon_count
    FROM geo g
    WHERE g.polygon IS NOT NULL
    GROUP BY g.type, g.name->>'uk'
    ORDER BY polygon_count DESC
    LIMIT 20
  `);
  console.log('\n4. Наші geo полігони:');
  for (const row of geoPolygons.rows) {
    console.log(`   ${row.type}: ${row.name_uk}`);
  }

  // 5. Try to resolve geo for sample OLX listings
  const sample = await client.query(`
    SELECT
      ul.id,
      ul.lat, ul.lng,
      primary_data->'location'->>'pathName' as path_name,
      g.id as found_geo_id,
      g.name->>'uk' as found_geo_name,
      g.type as found_geo_type
    FROM unified_listings ul
    LEFT JOIN geo g ON g.polygon IS NOT NULL
      AND ST_Contains(g.polygon, ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326))
    WHERE ul.realty_platform = 'olx'
    LIMIT 10
  `);
  console.log('\n5. Sample OLX з geo resolution:');
  for (const row of sample.rows) {
    console.log(`   [${row.lat}, ${row.lng}] path: ${row.path_name?.substring(0, 40)}`);
    console.log(`   → geo: ${row.found_geo_id ? `${row.found_geo_name} (${row.found_geo_type})` : 'НЕ ЗНАЙДЕНО'}`);
  }

  await client.end();
}

main().catch(console.error);
