const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Check streets from OSM
  const osm = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, osm_id
    FROM streets
    WHERE osm_id IS NOT NULL
    LIMIT 10
  `);

  console.log('OSM Streets:');
  for (const s of osm.rows) {
    console.log('  ID:', s.id, '| UK:', s.name_uk, '| RU:', s.name_ru);
  }

  // Check streets without OSM ID (from vector-api)
  const vector = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, osm_id
    FROM streets
    WHERE osm_id IS NULL
    LIMIT 10
  `);

  console.log('\nVector-API Streets (no OSM ID):');
  for (const s of vector.rows) {
    console.log('  ID:', s.id, '| UK:', s.name_uk, '| RU:', s.name_ru);
  }

  await client.end();
}

main().catch(console.error);
