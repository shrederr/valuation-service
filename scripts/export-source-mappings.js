/**
 * Export source_id_mappings from local DB to SQL file for server deployment.
 *
 * Output: SQL with TRUNCATE + batch INSERTs (safe for re-runs).
 * Usage: node scripts/export-source-mappings.js > data/server-sync/source_id_mappings.sql
 */
const { Client } = require('pg');

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
    SELECT source, entity_type, source_id, local_id, confidence, match_method
    FROM source_id_mappings
    WHERE source = 'vector2_crm'
    ORDER BY entity_type, source_id
  `);

  console.error(`Exporting ${rows.length} source_id_mappings...`);

  // Header
  console.log('-- source_id_mappings export');
  console.log(`-- Generated: ${new Date().toISOString()}`);
  console.log(`-- Total rows: ${rows.length}`);
  console.log('');
  console.log('BEGIN;');
  console.log('');
  console.log("DELETE FROM source_id_mappings WHERE source = 'vector2_crm';");
  console.log('');

  // Batch insert (500 rows per INSERT)
  const BATCH = 500;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    console.log('INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method) VALUES');
    const values = batch.map(r => {
      const method = r.match_method ? `'${r.match_method.replace(/'/g, "''")}'` : 'NULL';
      return `  ('${r.source}', '${r.entity_type}', ${r.source_id}, ${r.local_id}, ${r.confidence}, ${method})`;
    });
    console.log(values.join(',\n') + ';');
    console.log('');
  }

  console.log('COMMIT;');
  console.log('');

  // Stats
  const stats = {};
  for (const r of rows) {
    stats[r.entity_type] = (stats[r.entity_type] || 0) + 1;
  }
  console.log('-- Stats:');
  for (const [type, count] of Object.entries(stats)) {
    console.log(`--   ${type}: ${count}`);
  }

  await client.end();
  console.error('Done.');
}

main().catch(err => { console.error(err); process.exit(1); });
