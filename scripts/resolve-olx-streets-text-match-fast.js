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
  const regex = new RegExp(`(^|\\s|,|\\.|:|-)${escaped}($|\\s|,|\\.|:|-|\\d)`, 'i');
  return regex.test(normalizedText);
}

// Simple distance calculation (Haversine-like approximation for filtering)
function approxDistanceKm(lat1, lng1, lat2, lng2) {
  const latDiff = (lat2 - lat1) * 111.32; // km per degree at equator
  const lngDiff = (lng2 - lng1) * 111.32 * Math.cos(lat1 * Math.PI / 180);
  return Math.sqrt(latDiff * latDiff + lngDiff * lngDiff);
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX Street Text Matching (Fast) ===\n');

  // Load all streets into memory with centroid coordinates
  console.log('Loading all streets into memory...');
  const streetsResult = await client.query(`
    SELECT
      id,
      geo_id,
      name->>'uk' as name_uk,
      name->>'ru' as name_ru,
      ST_Y(ST_Centroid(line)) as lat,
      ST_X(ST_Centroid(line)) as lng
    FROM streets
    WHERE line IS NOT NULL
  `);

  const streets = streetsResult.rows.map(s => ({
    id: s.id,
    geo_id: s.geo_id,
    nameUk: s.name_uk,
    nameRu: s.name_ru,
    normalizedUk: normalizeStreetName(s.name_uk),
    normalizedRu: normalizeStreetName(s.name_ru),
    lat: parseFloat(s.lat),
    lng: parseFloat(s.lng)
  }));
  console.log(`Loaded ${streets.length} streets\n`);

  // Count OLX listings
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt
    FROM unified_listings
    WHERE realty_platform = 'olx'
      AND lat IS NOT NULL
      AND lng IS NOT NULL
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OLX listings to process: ${totalCount}\n`);

  let processed = 0;
  let textMatches = 0;
  let noTextMatch = 0;
  let offset = 0;
  const batchSize = 500;
  const startTime = Date.now();
  const maxDistanceKm = 5;

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

    // Process each listing
    const updates = [];
    for (const listing of listings.rows) {
      const listingLat = parseFloat(listing.lat);
      const listingLng = parseFloat(listing.lng);
      const descText = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).toLowerCase();

      // Skip if no description
      if (!descText.trim()) {
        noTextMatch++;
        continue;
      }

      // Find nearby streets (within 5km using simple distance)
      const nearbyStreets = streets
        .filter(s => approxDistanceKm(listingLat, listingLng, s.lat, s.lng) <= maxDistanceKm)
        .sort((a, b) => approxDistanceKm(listingLat, listingLng, a.lat, a.lng) - approxDistanceKm(listingLat, listingLng, b.lat, b.lng));

      // Try to find street name in description
      let matchedStreet = null;
      for (const street of nearbyStreets) {
        if (findStreetInText(street.normalizedUk, descText) || findStreetInText(street.normalizedRu, descText)) {
          matchedStreet = street;
          break;
        }
      }

      if (matchedStreet) {
        updates.push({
          id: listing.id,
          street_id: matchedStreet.id,
          geo_id: matchedStreet.geo_id
        });
        textMatches++;
      } else {
        noTextMatch++;
      }
    }

    // Batch update
    if (updates.length > 0) {
      const values = updates.map(u =>
        `('${u.id}'::uuid, ${u.street_id}, ${u.geo_id || 'NULL'})`
      ).join(',\n');

      await client.query(`
        UPDATE unified_listings ul
        SET street_id = v.street_id, geo_id = COALESCE(v.geo_id, ul.geo_id)
        FROM (VALUES ${values}) AS v(id, street_id, geo_id)
        WHERE ul.id = v.id
      `);
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

  // Verify with sample
  console.log('\nSample matches:');
  const sample = await client.query(`
    SELECT
      ul.description->>'uk' as desc,
      s.name->>'uk' as street
    FROM unified_listings ul
    JOIN streets s ON s.id = ul.street_id
    WHERE ul.realty_platform = 'olx'
      AND ul.description->>'uk' LIKE '%вул%'
    LIMIT 5
  `);
  for (const s of sample.rows) {
    console.log(`Street: ${s.street}`);
    console.log(`Desc: ${(s.desc || '').substring(0, 100)}...`);
    console.log('---');
  }

  await client.end();
}

main().catch(console.error);
