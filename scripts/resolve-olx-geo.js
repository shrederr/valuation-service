const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX GEO RESOLUTION (з пріоритетом типів) ===\n');

  // Check current state
  const before = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as with_geo
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('До:', before.rows[0]);

  // Resolve geo for OLX with priority: city_district > city > village > region_district > region
  // Using subquery to pick the most specific geo type
  console.log('\nЗапускаю geo resolution для OLX...');
  const startTime = Date.now();

  const result = await client.query(`
    UPDATE unified_listings ul
    SET geo_id = best_geo.id
    FROM (
      SELECT DISTINCT ON (ul2.id)
        ul2.id as listing_id,
        g.id,
        g.type,
        CASE g.type
          WHEN 'city_district' THEN 1
          WHEN 'city' THEN 2
          WHEN 'village' THEN 3
          WHEN 'region_district' THEN 4
          WHEN 'region' THEN 5
          ELSE 6
        END as priority
      FROM unified_listings ul2
      JOIN geo g ON g.polygon IS NOT NULL
        AND ST_Contains(g.polygon, ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326))
      WHERE ul2.realty_platform = 'olx'
        AND ul2.geo_id IS NULL
        AND ul2.lat IS NOT NULL
        AND ul2.lng IS NOT NULL
      ORDER BY ul2.id, priority ASC
    ) best_geo
    WHERE ul.id = best_geo.listing_id
  `);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Готово за ${elapsed}s, оновлено: ${result.rowCount}`);

  // Check result by type
  const byType = await client.query(`
    SELECT
      g.type,
      COUNT(*) as cnt
    FROM unified_listings ul
    JOIN geo g ON ul.geo_id = g.id
    WHERE ul.realty_platform = 'olx'
    GROUP BY g.type
    ORDER BY cnt DESC
  `);
  console.log('\nOLX по типах geo:');
  for (const row of byType.rows) {
    console.log(`  ${row.type}: ${row.cnt}`);
  }

  // Check after
  const after = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as with_geo,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('\nПісля:', after.rows[0]);

  await client.end();
}

main().catch(console.error);
