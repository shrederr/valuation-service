const { DataSource } = require('typeorm');
const ds = new DataSource({
  type: 'postgres',
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  username: 'postgres',
  password: 'postgres',
});
ds.initialize().then(async () => {
  // Check street matching flags
  const flags = await ds.query(`
    SELECT
      unnest(geo_resolution_flags) as flag,
      COUNT(*) as cnt
    FROM unified_listings
    WHERE geo_resolution_flags IS NOT NULL
    GROUP BY flag
    ORDER BY cnt DESC
  `);
  console.log('=== Street matching flags ===');
  flags.forEach(r => console.log(`  ${r.flag}: ${r.cnt}`));

  // Show examples with different methods
  console.log('\n=== Examples with text_parsed ===');
  const textParsed = await ds.query(`
    SELECT source_id, street_id, geo_resolution_flags
    FROM unified_listings
    WHERE 'street_by_text_parsed' = ANY(geo_resolution_flags)
    LIMIT 3
  `);
  textParsed.forEach(r => console.log(JSON.stringify(r)));

  console.log('\n=== Examples with text_found ===');
  const textFound = await ds.query(`
    SELECT source_id, street_id, geo_resolution_flags
    FROM unified_listings
    WHERE 'street_by_text_found' = ANY(geo_resolution_flags)
    LIMIT 3
  `);
  textFound.forEach(r => console.log(JSON.stringify(r)));

  console.log('\n=== Examples with nearest ===');
  const nearest = await ds.query(`
    SELECT source_id, street_id, geo_resolution_flags
    FROM unified_listings
    WHERE 'street_by_nearest' = ANY(geo_resolution_flags)
    LIMIT 3
  `);
  nearest.forEach(r => console.log(JSON.stringify(r)));

  await ds.destroy();
}).catch(e => console.error(e));
