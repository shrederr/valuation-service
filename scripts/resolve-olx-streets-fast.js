const { Pool } = require('pg');

async function main() {
  const pool = new Pool({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation',
    max: 5,
  });

  console.log('=== OLX Street Resolution - Fast Mode (Geometry) ===\n');

  const radiusMeters = parseInt(process.argv[2] || '200', 10);
  // Convert meters to degrees (approximate)
  // At Ukrainian latitude (~50°), 1 degree is about 71km longitude, 111km latitude
  const radiusDegrees = radiusMeters / 111000;

  console.log(`Radius: ${radiusMeters}m (~${radiusDegrees.toFixed(6)} degrees)`);

  const client = await pool.connect();

  const before = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('Before:', before.rows[0]);

  // Count remaining
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE realty_platform = 'olx' AND street_id IS NULL
      AND lat IS NOT NULL AND lng IS NOT NULL
  `);
  const totalRemaining = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Remaining: ${totalRemaining}`);

  console.log('\nRunning bulk update with geometry bounding box...');
  const startTime = Date.now();

  // Use geometry-based query with bounding box for speed
  const result = await client.query(`
    UPDATE unified_listings ul
    SET street_id = nearest.street_id
    FROM (
      SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
      FROM unified_listings ul2
      JOIN streets s ON s.line && ST_Expand(ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326), $1)
      WHERE ul2.realty_platform = 'olx'
        AND ul2.street_id IS NULL
        AND ul2.lat IS NOT NULL
        AND ul2.lng IS NOT NULL
        AND s.line IS NOT NULL
      ORDER BY ul2.id, ST_Distance(s.line, ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326))
    ) nearest
    WHERE ul.id = nearest.listing_id
  `, [radiusDegrees]);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Updated: ${result.rowCount} in ${elapsed}s`);

  client.release();

  const afterClient = await pool.connect();
  const after = await afterClient.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('After:', after.rows[0]);
  afterClient.release();

  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
