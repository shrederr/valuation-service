/**
 * Export apartment_complexes from local DB to SQL file for server sync.
 *
 * Uses INSERT ON CONFLICT DO UPDATE — safe for partial data on server.
 * Excludes polygon column (geometry) to keep SQL portable.
 *
 * Usage: node scripts/export-apartment-complexes.js > data/server-sync/apartment_complexes.sql
 */
const { Client } = require('pg');

function esc(val) {
  if (val === null || val === undefined) return 'NULL';
  return `'${String(val).replace(/'/g, "''")}'`;
}

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    user: 'postgres',
    password: 'postgres',
    database: 'valuation',
  });

  await client.connect();

  const { rows } = await client.query(`
    SELECT id, osm_id, osm_type, name_ru, name_uk, name_en, name_normalized,
           lat, lng, geo_id, street_id, topzone_id, source
    FROM apartment_complexes
    ORDER BY id
  `);

  console.error(`Exporting ${rows.length} apartment_complexes...`);

  console.log('-- apartment_complexes export');
  console.log(`-- Generated: ${new Date().toISOString()}`);
  console.log(`-- Total rows: ${rows.length}`);
  console.log('');
  console.log('BEGIN;');
  console.log('');

  const BATCH = 200;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    console.log(`INSERT INTO apartment_complexes (id, osm_id, osm_type, name_ru, name_uk, name_en, name_normalized, lat, lng, geo_id, street_id, topzone_id, source)`);
    console.log('VALUES');
    const values = batch.map(r => {
      return `  (${r.id}, ${r.osm_id || 'NULL'}, ${esc(r.osm_type)}, ${esc(r.name_ru)}, ${esc(r.name_uk)}, ${esc(r.name_en)}, ${esc(r.name_normalized)}, ${r.lat}, ${r.lng}, ${r.geo_id || 'NULL'}, ${r.street_id || 'NULL'}, ${r.topzone_id || 'NULL'}, ${esc(r.source)})`;
    });
    console.log(values.join(',\n'));
    console.log(`ON CONFLICT (id) DO UPDATE SET`);
    console.log(`  osm_id = EXCLUDED.osm_id, osm_type = EXCLUDED.osm_type,`);
    console.log(`  name_ru = EXCLUDED.name_ru, name_uk = EXCLUDED.name_uk, name_en = EXCLUDED.name_en,`);
    console.log(`  name_normalized = EXCLUDED.name_normalized,`);
    console.log(`  lat = EXCLUDED.lat, lng = EXCLUDED.lng,`);
    console.log(`  geo_id = EXCLUDED.geo_id, street_id = EXCLUDED.street_id, topzone_id = EXCLUDED.topzone_id,`);
    console.log(`  source = EXCLUDED.source;`);
    console.log('');
  }

  // Fix sequence
  const maxId = rows.length > 0 ? Math.max(...rows.map(r => r.id)) : 0;
  if (maxId > 0) {
    console.log(`SELECT setval('apartment_complexes_id_seq', ${maxId}, true);`);
    console.log('');
  }

  console.log('COMMIT;');
  console.log('');

  console.log(`-- Max ID: ${maxId}`);
  console.log(`-- Total: ${rows.length}`);

  await client.end();
  console.error('Done.');
}

main().catch(err => { console.error(err); process.exit(1); });
