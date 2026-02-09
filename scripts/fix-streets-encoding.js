const { Client } = require('pg');

// Try to fix mojibake - UTF-8 read as Windows-1251
function fixEncoding(str) {
  if (!str) return str;
  try {
    // Convert string to Buffer assuming it's Windows-1251 encoded
    // Then decode as UTF-8
    const buffer = Buffer.from(str, 'latin1');
    const fixed = buffer.toString('utf8');
    return fixed;
  } catch (e) {
    return str;
  }
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Test on a few samples first
  const samples = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM streets
    WHERE osm_id IS NOT NULL
    LIMIT 10
  `);

  console.log('Testing encoding fix:\n');
  for (const s of samples.rows) {
    const fixedUk = fixEncoding(s.name_uk);
    const fixedRu = fixEncoding(s.name_ru);
    console.log('Original UK:', s.name_uk);
    console.log('Fixed UK:   ', fixedUk);
    console.log('Original RU:', s.name_ru);
    console.log('Fixed RU:   ', fixedRu);
    console.log('---');
  }

  await client.end();
}

main().catch(console.error);
