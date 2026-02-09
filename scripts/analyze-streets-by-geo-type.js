const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('Analysis: Street coverage by geo type\n');

  // Check listings by geo type with street coverage
  const byGeoType = await client.query(`
    SELECT
      g.type as geo_type,
      COUNT(ul.id) as total_listings,
      COUNT(ul.id) FILTER (WHERE ul.street_id IS NOT NULL) as with_street,
      COUNT(ul.id) FILTER (WHERE ul.street_id IS NULL) as no_street,
      ROUND(100.0 * COUNT(ul.id) FILTER (WHERE ul.street_id IS NOT NULL) / NULLIF(COUNT(ul.id), 0), 1) as pct_with_street
    FROM unified_listings ul
    JOIN geo g ON ul.geo_id = g.id
    GROUP BY g.type
    ORDER BY total_listings DESC
  `);

  console.log('By geo type:');
  console.log('Type              | Total     | With Street | No Street  | Coverage');
  console.log('-'.repeat(75));
  for (const row of byGeoType.rows) {
    console.log(
      `${(row.geo_type || 'null').padEnd(17)} | ${row.total_listings.toString().padStart(9)} | ${row.with_street.toString().padStart(11)} | ${row.no_street.toString().padStart(10)} | ${(row.pct_with_street || 0)}%`
    );
  }

  // Check listings without geo_id
  const noGeo = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE geo_id IS NULL
  `);
  console.log(`\nNo geo_id         | ${noGeo.rows[0].total.toString().padStart(9)} | ${noGeo.rows[0].with_street.toString().padStart(11)} | - | 0%`);

  // Detailed breakdown for non-city types
  console.log('\n\nDetailed: Non-city geo types without streets:');
  const nonCityDetails = await client.query(`
    SELECT
      g.type as geo_type,
      g.name->>'uk' as geo_name,
      COUNT(ul.id) as no_street_count
    FROM unified_listings ul
    JOIN geo g ON ul.geo_id = g.id
    WHERE ul.street_id IS NULL
      AND g.type NOT IN ('city', 'city_district')
    GROUP BY g.type, g.name->>'uk'
    ORDER BY no_street_count DESC
    LIMIT 20
  `);

  console.log('Geo Type          | Geo Name                           | No Street');
  console.log('-'.repeat(75));
  for (const row of nonCityDetails.rows) {
    console.log(
      `${(row.geo_type || 'null').padEnd(17)} | ${(row.geo_name || 'null').substring(0, 34).padEnd(34)} | ${row.no_street_count}`
    );
  }

  // Summary: cities vs non-cities
  console.log('\n\nSummary: Cities vs Non-cities');
  const summary = await client.query(`
    SELECT
      CASE
        WHEN g.type IN ('city', 'city_district') THEN 'Cities'
        ELSE 'Non-cities (villages, etc.)'
      END as category,
      COUNT(ul.id) as total,
      COUNT(ul.id) FILTER (WHERE ul.street_id IS NOT NULL) as with_street,
      COUNT(ul.id) FILTER (WHERE ul.street_id IS NULL) as no_street
    FROM unified_listings ul
    JOIN geo g ON ul.geo_id = g.id
    GROUP BY
      CASE
        WHEN g.type IN ('city', 'city_district') THEN 'Cities'
        ELSE 'Non-cities (villages, etc.)'
      END
  `);

  for (const row of summary.rows) {
    const pct = ((row.with_street / row.total) * 100).toFixed(1);
    console.log(`${row.category}: ${row.total} total, ${row.with_street} with street (${pct}%), ${row.no_street} without`);
  }

  await client.end();
}

main().catch(console.error);
