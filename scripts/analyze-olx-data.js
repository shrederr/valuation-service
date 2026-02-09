const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== АНАЛІЗ OLX ДАНИХ ===\n');

  // 1. Check if OLX has coordinates
  const coords = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE lat IS NOT NULL AND lng IS NOT NULL) as with_coords,
      COUNT(*) FILTER (WHERE lat IS NULL OR lng IS NULL) as no_coords
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('1. Координати OLX:');
  console.log(`   Всього: ${coords.rows[0].total}`);
  console.log(`   З координатами: ${coords.rows[0].with_coords}`);
  console.log(`   Без координат: ${coords.rows[0].no_coords}`);

  // 2. Check primaryData keys
  const keys = await client.query(`
    SELECT key, COUNT(*) as cnt
    FROM unified_listings,
    LATERAL jsonb_object_keys(primary_data::jsonb) as key
    WHERE realty_platform = 'olx' AND primary_data IS NOT NULL
    GROUP BY key
    ORDER BY cnt DESC
    LIMIT 20
  `);
  console.log('\n2. Ключі в primaryData OLX:');
  for (const row of keys.rows) {
    console.log(`   ${row.key}: ${row.cnt}`);
  }

  // 3. Check location structure
  const location = await client.query(`
    SELECT
      primary_data->'location'->>'city' as city,
      primary_data->'location'->>'region' as region,
      primary_data->'location'->>'pathName' as path_name
    FROM unified_listings
    WHERE realty_platform = 'olx' AND primary_data->'location' IS NOT NULL
    LIMIT 5
  `);
  console.log('\n3. Приклади location:');
  for (const row of location.rows) {
    console.log(`   city: ${row.city}, region: ${row.region}`);
    console.log(`   pathName: ${row.path_name}`);
    console.log('   ---');
  }

  // 4. Check title examples
  const titles = await client.query(`
    SELECT
      primary_data->>'title' as title
    FROM unified_listings
    WHERE realty_platform = 'olx' AND primary_data->>'title' IS NOT NULL
    LIMIT 10
  `);
  console.log('\n4. Приклади title:');
  for (const row of titles.rows) {
    console.log(`   ${row.title}`);
  }

  // 5. Check description examples
  const descs = await client.query(`
    SELECT
      LEFT(description->>'uk', 200) as desc_uk
    FROM unified_listings
    WHERE realty_platform = 'olx' AND description->>'uk' IS NOT NULL
    LIMIT 5
  `);
  console.log('\n5. Приклади description (перші 200 символів):');
  for (const row of descs.rows) {
    console.log(`   ${row.desc_uk}`);
    console.log('   ---');
  }

  // 6. Check params structure (OLX specific)
  const params = await client.query(`
    SELECT
      primary_data->'params' as params
    FROM unified_listings
    WHERE realty_platform = 'olx' AND primary_data->'params' IS NOT NULL
    LIMIT 3
  `);
  console.log('\n6. Приклади params:');
  for (const row of params.rows) {
    console.log(`   ${JSON.stringify(row.params).substring(0, 300)}...`);
  }

  // 7. Unique cities in OLX
  const cities = await client.query(`
    SELECT
      primary_data->'location'->>'city' as city,
      COUNT(*) as cnt
    FROM unified_listings
    WHERE realty_platform = 'olx' AND primary_data->'location'->>'city' IS NOT NULL
    GROUP BY primary_data->'location'->>'city'
    ORDER BY cnt DESC
    LIMIT 15
  `);
  console.log('\n7. Топ міст OLX:');
  for (const row of cities.rows) {
    console.log(`   ${row.city}: ${row.cnt}`);
  }

  await client.end();
}

main().catch(console.error);
