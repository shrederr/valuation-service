const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('Connected to Railway DB');

  // Step 1: Update streets geo_id based on their centroid falling within geo polygons
  console.log('\nStep 1: Updating streets geo_id based on polygon containment...');
  const startTime = Date.now();

  const updateResult = await client.query(`
    UPDATE streets s
    SET geo_id = g.id
    FROM geo g
    WHERE g.polygon IS NOT NULL
      AND ST_Contains(g.polygon, ST_Centroid(s.line))
  `);

  const elapsed1 = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Done in ${elapsed1}s, rows affected: ${updateResult.rowCount}`);

  // Check overlap now
  const overlap = await client.query(`
    SELECT COUNT(DISTINCT s.geo_id) as matching_geos
    FROM streets s
    WHERE s.geo_id IN (SELECT DISTINCT geo_id FROM unified_listings WHERE geo_id IS NOT NULL)
  `);
  console.log('Matching geo_ids now:', overlap.rows[0]);

  // Step 2: Re-run street resolution with geo_id restriction (fast)
  console.log('\nStep 2: Running fast street resolution (200m, with geo_id)...');
  const start2 = Date.now();

  const streetResult = await client.query(`
    UPDATE unified_listings ul
    SET street_id = nearest.street_id
    FROM (
      SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
      FROM unified_listings ul2
      CROSS JOIN LATERAL (
        SELECT s.id
        FROM streets s
        WHERE s.geo_id = ul2.geo_id
          AND ST_DWithin(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
            200
          )
        ORDER BY ST_Distance(
          s.line::geography,
          ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
        )
        LIMIT 1
      ) s
      WHERE ul2.street_id IS NULL
        AND ul2.geo_id IS NOT NULL
        AND ul2.lat IS NOT NULL
        AND ul2.lng IS NOT NULL
    ) nearest
    WHERE ul.id = nearest.listing_id
  `);

  const elapsed2 = ((Date.now() - start2) / 1000).toFixed(1);
  console.log(`Done in ${elapsed2}s, rows affected: ${streetResult.rowCount}`);

  // Final stats
  const final = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      COUNT(*) FILTER (WHERE geo_id IS NOT NULL) as with_geo
    FROM unified_listings
  `);
  console.log('\nFinal stats:', final.rows[0]);

  await client.end();
}

main().catch(console.error);
