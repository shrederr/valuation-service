const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Find and cancel long-running street resolution queries
  const queries = await client.query(`
    SELECT pid, query_start, now() - query_start as duration, left(query, 50) as query_start_text
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query LIKE '%UPDATE unified_listings%street_id%nearest%'
      AND now() - query_start > interval '1 minute'
  `);

  console.log('Long-running street resolution queries:', queries.rowCount);

  for (const q of queries.rows) {
    console.log(`Cancelling PID ${q.pid} (running for ${q.duration})...`);
    try {
      await client.query('SELECT pg_cancel_backend($1)', [q.pid]);
      console.log(`  Cancelled PID ${q.pid}`);
    } catch (e) {
      console.log(`  Error cancelling: ${e.message}`);
    }
  }

  // Also cancel the batch queries
  const batchQueries = await client.query(`
    SELECT pid, query_start, now() - query_start as duration
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query LIKE '%WITH batch AS%unified_listings%'
      AND now() - query_start > interval '1 minute'
  `);

  for (const q of batchQueries.rows) {
    console.log(`Cancelling batch PID ${q.pid}...`);
    try {
      await client.query('SELECT pg_cancel_backend($1)', [q.pid]);
      console.log(`  Cancelled PID ${q.pid}`);
    } catch (e) {
      console.log(`  Error cancelling: ${e.message}`);
    }
  }

  console.log('\nDone. Checking remaining active queries...');

  const remaining = await client.query(`
    SELECT count(*) as cnt
    FROM pg_stat_activity
    WHERE state = 'active'
      AND (query LIKE '%UPDATE unified_listings%street_id%nearest%'
           OR query LIKE '%WITH batch AS%unified_listings%')
  `);
  console.log('Remaining street queries:', remaining.rows[0].cnt);

  await client.end();
}

main().catch(console.error);
