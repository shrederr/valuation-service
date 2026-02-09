const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Try PostgreSQL CONVERT to fix encoding
  // The data looks like UTF-8 bytes interpreted as LATIN1
  const samples = await client.query(`
    SELECT
      id,
      name->>'uk' as original_uk,
      convert_from(convert_to(name->>'uk', 'LATIN1'), 'UTF8') as fixed_uk
    FROM streets
    WHERE osm_id IS NOT NULL
    LIMIT 10
  `);

  console.log('Testing PostgreSQL encoding fix:\n');
  for (const s of samples.rows) {
    console.log('Original:', s.original_uk);
    console.log('Fixed:   ', s.fixed_uk);
    console.log('---');
  }

  await client.end();
}

main().catch(console.error);
