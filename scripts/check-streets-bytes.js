const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Get raw bytes
  const samples = await client.query(`
    SELECT
      id,
      name->>'uk' as name_uk,
      encode(convert_to(name->>'uk', 'UTF8'), 'hex') as hex_uk
    FROM streets
    WHERE osm_id IS NOT NULL
    LIMIT 5
  `);

  console.log('Street names with hex:\n');
  for (const s of samples.rows) {
    console.log('ID:', s.id);
    console.log('Name UK:', s.name_uk);
    console.log('Hex:', s.hex_uk);

    // Try to decode the hex as UTF-8
    try {
      const buffer = Buffer.from(s.hex_uk, 'hex');
      console.log('Decoded:', buffer.toString('utf8'));
    } catch (e) {
      console.log('Decode error:', e.message);
    }
    console.log('---');
  }

  // Also check a listing description for comparison
  const listing = await client.query(`
    SELECT description->>'uk' as desc_uk
    FROM unified_listings
    WHERE realty_platform = 'olx' AND description->>'uk' IS NOT NULL
    LIMIT 1
  `);

  if (listing.rows.length > 0) {
    console.log('\nListing description (first 100 chars):');
    console.log(listing.rows[0].desc_uk.substring(0, 100));
  }

  await client.end();
}

main().catch(console.error);
