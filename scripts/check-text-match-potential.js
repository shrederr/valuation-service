const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Get sample of OLX listings WITH street_id
  // Check if street name appears in title/description
  const listings = await client.query(`
    SELECT
      ul.id,
      primary_data->>'title' as title,
      description->>'uk' as description_uk,
      s.name->>'uk' as street_name_uk,
      s.name->>'ru' as street_name_ru
    FROM unified_listings ul
    JOIN streets s ON ul.street_id = s.id
    WHERE ul.realty_platform = 'olx'
    LIMIT 20
  `);

  console.log('Checking if street names appear in OLX title/description:\n');

  let titleMatches = 0;
  let descMatches = 0;
  let noMatches = 0;

  for (const l of listings.rows) {
    const title = (l.title || '').toLowerCase();
    const desc = (l.description_uk || '').toLowerCase();

    // Normalize street name
    const streetUk = (l.street_name_uk || '')
      .toLowerCase()
      .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
      .replace(/[«»""''`']/g, '')
      .trim();

    const streetRu = (l.street_name_ru || '')
      .toLowerCase()
      .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
      .replace(/[«»""''`']/g, '')
      .trim();

    const inTitle = (streetUk && title.includes(streetUk)) || (streetRu && title.includes(streetRu));
    const inDesc = (streetUk && desc.includes(streetUk)) || (streetRu && desc.includes(streetRu));

    if (inTitle) {
      titleMatches++;
      console.log('TITLE MATCH:', l.street_name_uk);
      console.log('  Title:', l.title?.substring(0, 100));
    } else if (inDesc) {
      descMatches++;
      console.log('DESC MATCH:', l.street_name_uk);
      console.log('  Desc:', l.description_uk?.substring(0, 100));
    } else {
      noMatches++;
      console.log('NO MATCH:', l.street_name_uk);
      console.log('  Title:', l.title?.substring(0, 100) || 'null');
      console.log('  Desc:', l.description_uk?.substring(0, 100) || 'null');
    }
    console.log('---');
  }

  console.log('\nSummary:');
  console.log('  Title matches:', titleMatches);
  console.log('  Desc matches:', descMatches);
  console.log('  No matches:', noMatches);

  await client.end();
}

main().catch(console.error);
