const { Client } = require('pg');

async function main() {
  // Check local first
  const local = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });

  await local.connect();

  const l1 = await local.query('SELECT COUNT(*) as total FROM streets');
  const l2 = await local.query('SELECT COUNT(*) as with_line FROM streets WHERE line IS NOT NULL');

  console.log('Local DB - streets table:');
  console.log('  Total streets:', l1.rows[0].total);
  console.log('  With geometry (line IS NOT NULL):', l2.rows[0].with_line);
  console.log('  Without geometry:', l1.rows[0].total - l2.rows[0].with_line);

  await local.end();

  // Now try Railway
  console.log('\nConnecting to Railway DB...');
  try {
    const c = new Client({
      host: 'junction.proxy.rlwy.net',
      port: 43987,
      database: 'railway',
      user: 'postgres',
      password: 'ClLJHsXiTFRzZiGJBUfMiCPCQkRLPHkL',
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 30000,
    });

    await c.connect();

    const r1 = await c.query('SELECT COUNT(*) as total FROM streets');
    const r2 = await c.query('SELECT COUNT(*) as with_line FROM streets WHERE line IS NOT NULL');

    console.log('\nRailway DB - streets table:');
    console.log('  Total streets:', r1.rows[0].total);
    console.log('  With geometry (line IS NOT NULL):', r2.rows[0].with_line);
    console.log('  Without geometry:', r1.rows[0].total - r2.rows[0].with_line);

    await c.end();
  } catch (err) {
    console.log('\nRailway connection error:', err.message);
  }
}

main().catch(console.error);
