const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Check streets geo_id distribution
  const streetGeo = await client.query(`
    SELECT
      COUNT(*) as total_streets,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as streets_with_geo
    FROM streets
  `);
  console.log('Streets:', streetGeo.rows[0]);

  // Check unique geo_ids in streets vs listings
  const geoComparison = await client.query(`
    SELECT
      (SELECT COUNT(DISTINCT geo_id) FROM streets WHERE geo_id IS NOT NULL) as street_geos,
      (SELECT COUNT(DISTINCT geo_id) FROM unified_listings WHERE geo_id IS NOT NULL) as listing_geos
  `);
  console.log('Unique geo_ids:', geoComparison.rows[0]);

  // Check overlap of geo_ids
  const overlap = await client.query(`
    SELECT COUNT(DISTINCT s.geo_id) as matching_geos
    FROM streets s
    WHERE s.geo_id IN (SELECT DISTINCT geo_id FROM unified_listings WHERE geo_id IS NOT NULL)
  `);
  console.log('Matching geo_ids (streets that have listings):', overlap.rows[0]);

  // Check listings without matching street geo_id
  const noMatch = await client.query(`
    SELECT COUNT(*) as listings_without_street_match
    FROM unified_listings ul
    WHERE ul.geo_id IS NOT NULL
      AND ul.street_id IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM streets s WHERE s.geo_id = ul.geo_id
      )
  `);
  console.log('Listings with geo_id but NO streets in that geo:', noMatch.rows[0]);

  await client.end();
}

main().catch(console.error);
