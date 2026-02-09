const { Client } = require('pg');

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });
  await client.connect();

  const activity = await client.query(`
    SELECT pid, state, query_start, LEFT(query, 150) as query
    FROM pg_stat_activity
    WHERE datname = 'valuation'
      AND state = 'active'
      AND pid != pg_backend_pid()
  `);

  console.log('Активные запросы:');
  activity.rows.forEach(r => {
    console.log('PID:', r.pid, '| State:', r.state);
    console.log('Started:', r.query_start);
    console.log('Query:', r.query);
    console.log('---');
  });

  if (activity.rows.length === 0) {
    console.log('(нет активных запросов)');
  }

  await client.end();
}
main().catch(console.error);
