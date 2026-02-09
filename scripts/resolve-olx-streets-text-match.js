const { Client } = require('pg');

// Normalize street name for matching - remove prefixes
function normalizeStreetName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
    .replace(/[«»""''`']/g, '')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

// Check if normalized street name appears in normalized text
function findStreetInText(normalizedStreet, normalizedText) {
  if (!normalizedStreet || normalizedStreet.length < 3) return false;
  if (!normalizedText) return false;

  // Direct substring match
  if (normalizedText.includes(normalizedStreet)) return true;

  // Word boundary match
  const escaped = normalizedStreet.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(^|\\s|,|\\.|:)${escaped}($|\\s|,|\\.|:|\\d)`, 'i');
  return regex.test(normalizedText);
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX Street Text Matching ===\n');

  // Count OLX listings without street (or with coordinate-matched street that we want to re-check)
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt
    FROM unified_listings
    WHERE realty_platform = 'olx'
      AND lat IS NOT NULL
      AND lng IS NOT NULL
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OLX listings to process: ${totalCount}`);

  let processed = 0;
  let textMatches = 0;
  let noTextMatch = 0;
  let offset = 0;
  const batchSize = 200;
  const startTime = Date.now();
  const radiusDegrees = 0.045; // ~5km

  while (processed < totalCount) {
    // Fetch batch of listings
    const listings = await client.query(`
      SELECT
        id,
        lat,
        lng,
        description->>'uk' as desc_uk,
        description->>'ru' as desc_ru
      FROM unified_listings
      WHERE realty_platform = 'olx'
        AND lat IS NOT NULL
        AND lng IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (listings.rows.length === 0) break;

    // For each listing, find streets within 5km and try text matching
    for (const listing of listings.rows) {
      // Get streets within 5km radius
      const streets = await client.query(`
        SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, geo_id
        FROM streets
        WHERE line && ST_Expand(ST_SetSRID(ST_MakePoint($1, $2), 4326), $3)
        ORDER BY ST_Distance(line, ST_SetSRID(ST_MakePoint($1, $2), 4326))
        LIMIT 100
      `, [listing.lng, listing.lat, radiusDegrees]);

      // Combine all description text
      const descText = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).toLowerCase();

      let matchedStreet = null;

      // Try to match street names in description
      for (const street of streets.rows) {
        const nameUk = normalizeStreetName(street.name_uk);
        const nameRu = normalizeStreetName(street.name_ru);

        if (findStreetInText(nameUk, descText) || findStreetInText(nameRu, descText)) {
          matchedStreet = street;
          break;
        }
      }

      // Update listing with matched street (and its geo_id)
      if (matchedStreet) {
        await client.query(`
          UPDATE unified_listings
          SET street_id = $1, geo_id = COALESCE($2, geo_id)
          WHERE id = $3
        `, [matchedStreet.id, matchedStreet.geo_id, listing.id]);
        textMatches++;
      } else {
        noTextMatch++;
      }
    }

    processed += listings.rows.length;
    offset += batchSize;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const rate = Math.round(processed / (parseFloat(elapsed) || 1));
    console.log(`Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | TextMatch: ${textMatches} | NoMatch: ${noTextMatch} | ${elapsed}s (${rate}/s)`);
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nCompleted in ${totalTime}s`);
  console.log(`Text matches: ${textMatches} (${((textMatches/totalCount)*100).toFixed(1)}%)`);
  console.log(`No text match: ${noTextMatch} (${((noTextMatch/totalCount)*100).toFixed(1)}%)`);

  await client.end();
}

main().catch(console.error);
