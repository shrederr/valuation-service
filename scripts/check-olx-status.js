const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Check current stats
  const stats = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NOT NULL) as with_street,
      COUNT(*) FILTER (WHERE street_id IS NULL) as without_street
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  console.log('OLX current stats:', stats.rows[0]);

  // Check if there are any active queries
  const queries = await client.query(`
    SELECT pid, state, query_start, now() - query_start as duration, left(query, 100) as query_start_text
    FROM pg_stat_activity
    WHERE state = 'active' AND query LIKE '%unified_listings%'
  `);
  console.log('\nActive queries on unified_listings:', queries.rowCount);
  for (const q of queries.rows) {
    console.log('  PID:', q.pid, '| Duration:', q.duration);
    console.log('  Query:', q.query_start_text);
  }

  await client.end();
}

main().catch(console.error);
