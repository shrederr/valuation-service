const { Client } = require('pg');

async function run() {
  let retries = 3;

  while (retries > 0) {
    const client = new Client({
      connectionString: 'postgresql://postgres:sGPLBPoVYgQzOnfxNdAKaFzksrNCAulB@junction.proxy.rlwy.net:40863/railway',
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 30000,
    });

    try {
      await client.connect();

      const dupes = await client.query(`
        SELECT source_type, source_id, COUNT(*) as cnt
        FROM unified_listings
        GROUP BY source_type, source_id
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 20
      `);
      console.log('Duplicate source_id entries:', dupes.rows.length);
      if (dupes.rows.length > 0) console.table(dupes.rows);

      const totalDupes = await client.query(`
        SELECT COALESCE(SUM(cnt - 1), 0) as extra_rows FROM (
          SELECT COUNT(*) as cnt
          FROM unified_listings
          GROUP BY source_type, source_id
          HAVING COUNT(*) > 1
        ) sub
      `);
      console.log('Total extra duplicate rows:', totalDupes.rows[0].extra_rows);

      const total = await client.query('SELECT source_type, COUNT(*) FROM unified_listings GROUP BY source_type');
      console.log('Counts by source:');
      console.table(total.rows);

      await client.end();
      return;
    } catch (e) {
      retries--;
      console.error('Error:', e.message, '- retries left:', retries);
      try { await client.end(); } catch {}
      if (retries === 0) process.exit(1);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

run();
