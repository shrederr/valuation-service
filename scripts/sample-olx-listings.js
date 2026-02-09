const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Get sample of OLX listings that still don't have street_id
  const listings = await client.query(`
    SELECT
      id,
      primary_data->>'title' as title,
      description->>'uk' as description_uk,
      lat, lng
    FROM unified_listings
    WHERE realty_platform = 'olx' AND street_id IS NULL
      AND lat IS NOT NULL AND lng IS NOT NULL
    LIMIT 10
  `);

  console.log('Sample OLX listings without street_id:\n');
  for (const l of listings.rows) {
    console.log('ID:', l.id);
    console.log('Title:', l.title);
    console.log('Description (first 200):', (l.description_uk || '').substring(0, 200));
    console.log('Coords:', l.lat, l.lng);
    console.log('---');
  }

  // Also check some that already HAVE street_id to compare
  const withStreet = await client.query(`
    SELECT
      id,
      primary_data->>'title' as title,
      description->>'uk' as description_uk,
      s.name->>'uk' as street_name
    FROM unified_listings ul
    JOIN streets s ON ul.street_id = s.id
    WHERE ul.realty_platform = 'olx'
    LIMIT 5
  `);

  console.log('\n\nSample OLX listings WITH street_id:\n');
  for (const l of withStreet.rows) {
    console.log('Title:', l.title);
    console.log('Street:', l.street_name);
    console.log('Description (first 200):', (l.description_uk || '').substring(0, 200));
    console.log('---');
  }

  await client.end();
}

main().catch(console.error);
