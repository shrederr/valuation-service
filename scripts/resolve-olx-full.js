const { Client } = require('pg');

// ====== STREET MATCHING ======

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

function findStreetInText(normalizedStreet, normalizedText) {
  if (!normalizedStreet || normalizedStreet.length < 3) return false;
  if (!normalizedText) return false;
  if (normalizedText.includes(normalizedStreet)) return true;
  const escaped = normalizedStreet.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(^|\\s|,|\\.|:|-)${escaped}($|\\s|,|\\.|:|-|\\d)`, 'i');
  return regex.test(normalizedText);
}

function approxDistanceKm(lat1, lng1, lat2, lng2) {
  const latDiff = (lat2 - lat1) * 111.32;
  const lngDiff = (lng2 - lng1) * 111.32 * Math.cos(lat1 * Math.PI / 180);
  return Math.sqrt(latDiff * latDiff + lngDiff * lngDiff);
}

// ====== COMPLEX MATCHING ======

const COMPLEX_BLACKLIST = new Set([
  'сільпо', 'novus', 'varus', 'атб', 'атб-маркет', 'фора', 'ашан',
  'котельня', 'теплопункт', 'підстанція', 'трансформаторна',
  'ангар', 'склад', 'гараж', 'garage', 'паркінг',
  'продукти', 'продуктовий', 'аптека', 'пошта', 'нова пошта',
  'магазин', 'супермаркет', 'гіпермаркет',
  'поліція', 'військомат', 'комісаріат',
  'школа', 'садок', 'ліцей', 'гімназія', 'університет',
  'церква', 'храм', 'собор', 'мечеть', 'синагога',
  'лікарня', 'поліклініка', 'медичний', 'стоматологія',
  'ресторан', 'кафе', 'бар', 'паб', 'піцерія',
  'корпус', 'будинок', 'секція', 'блок', 'буд',
  'олімп', 'старт', 'динамо', 'спартак',
]);

function normalizeComplexName(name) {
  if (!name) return '';
  return name
    .replace(/^(жк|житловий комплекс|жилой комплекс|кг|км|котеджне містечко|коттеджный городок)\s+/gi, '')
    .replace(/[«»""''`'()]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX Full Resolution: Streets + Complexes ===\n');

  // ====== LOAD STREETS ======
  console.log('Loading streets...');
  const streetsResult = await client.query(`
    SELECT id, geo_id, name->>'uk' as name_uk, name->>'ru' as name_ru,
           ST_Y(ST_Centroid(line)) as lat, ST_X(ST_Centroid(line)) as lng
    FROM streets WHERE line IS NOT NULL
  `);

  const streets = streetsResult.rows.map(s => ({
    id: s.id,
    geo_id: s.geo_id,
    normalizedUk: normalizeStreetName(s.name_uk),
    normalizedRu: normalizeStreetName(s.name_ru),
    lat: parseFloat(s.lat),
    lng: parseFloat(s.lng)
  }));
  console.log(`Loaded ${streets.length} streets`);

  // ====== LOAD COMPLEXES ======
  console.log('Loading complexes...');
  const complexesResult = await client.query(`
    SELECT id, name_ru, name_uk, lat, lng, geo_id, street_id
    FROM apartment_complexes
    WHERE lat IS NOT NULL AND lng IS NOT NULL
  `);

  const complexMap = new Map(); // normalized name -> complex data
  for (const c of complexesResult.rows) {
    const nameRu = normalizeComplexName(c.name_ru);
    const nameUk = normalizeComplexName(c.name_uk);

    const complex = {
      id: c.id,
      nameRu: c.name_ru,
      nameUk: c.name_uk,
      lat: parseFloat(c.lat),
      lng: parseFloat(c.lng),
      geo_id: c.geo_id,
      street_id: c.street_id
    };

    if (nameRu && nameRu.length >= 3 && !COMPLEX_BLACKLIST.has(nameRu)) {
      complexMap.set(nameRu, complex);
    }
    if (nameUk && nameUk.length >= 3 && !COMPLEX_BLACKLIST.has(nameUk)) {
      complexMap.set(nameUk, complex);
    }
  }
  console.log(`Loaded ${complexMap.size} unique complex names\n`);

  // Build complex search pattern
  const sortedComplexNames = [...complexMap.keys()].sort((a, b) => b.length - a.length);
  const escapedComplexNames = sortedComplexNames.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const complexPattern = new RegExp(
    `(?:жк|житловий комплекс|жилой комплекс|кг|км)\\s*["«']?(${escapedComplexNames.join('|')})["»']?`,
    'gi'
  );

  // ====== COUNT OLX LISTINGS ======
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE realty_platform = 'olx' AND lat IS NOT NULL AND lng IS NOT NULL
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OLX listings: ${totalCount}\n`);

  // ====== PROCESS ======
  let processed = 0;
  let streetMatches = 0;
  let complexMatches = 0;
  let noMatch = 0;
  let offset = 0;
  const batchSize = 500;
  const startTime = Date.now();
  const maxDistanceKm = 5;

  while (processed < totalCount) {
    const listings = await client.query(`
      SELECT id, lat, lng, description->>'uk' as desc_uk, description->>'ru' as desc_ru
      FROM unified_listings
      WHERE realty_platform = 'olx' AND lat IS NOT NULL AND lng IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (listings.rows.length === 0) break;

    const updates = [];

    for (const listing of listings.rows) {
      const listingLat = parseFloat(listing.lat);
      const listingLng = parseFloat(listing.lng);
      const descText = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).toLowerCase();

      if (!descText.trim()) {
        noMatch++;
        continue;
      }

      let matched = false;

      // ====== STEP 1: Try street text matching ======
      const nearbyStreets = streets
        .filter(s => approxDistanceKm(listingLat, listingLng, s.lat, s.lng) <= maxDistanceKm)
        .sort((a, b) => approxDistanceKm(listingLat, listingLng, a.lat, a.lng) - approxDistanceKm(listingLat, listingLng, b.lat, b.lng));

      for (const street of nearbyStreets) {
        if (findStreetInText(street.normalizedUk, descText) || findStreetInText(street.normalizedRu, descText)) {
          updates.push({
            id: listing.id,
            street_id: street.id,
            geo_id: street.geo_id,
            complex_id: null
          });
          streetMatches++;
          matched = true;
          break;
        }
      }

      // ====== STEP 2: If no street, try complex text matching ======
      if (!matched) {
        complexPattern.lastIndex = 0;
        const match = complexPattern.exec(descText);

        if (match) {
          const matchedName = match[1].toLowerCase();
          const complex = complexMap.get(matchedName);

          if (complex) {
            // Check distance - complex should be within reasonable range
            const distToComplex = approxDistanceKm(listingLat, listingLng, complex.lat, complex.lng);
            if (distToComplex <= 10) { // 10km max for complex match
              // Find nearest street to complex coordinates (since complexes have no geo_id/street_id)
              const nearbyStreetsToComplex = streets
                .filter(s => approxDistanceKm(complex.lat, complex.lng, s.lat, s.lng) <= 2)
                .sort((a, b) => approxDistanceKm(complex.lat, complex.lng, a.lat, a.lng) - approxDistanceKm(complex.lat, complex.lng, b.lat, b.lng));

              const nearestStreet = nearbyStreetsToComplex[0];

              updates.push({
                id: listing.id,
                street_id: nearestStreet ? nearestStreet.id : null,
                geo_id: nearestStreet ? nearestStreet.geo_id : null,
                complex_id: complex.id
              });
              complexMatches++;
              matched = true;
            }
          }
        }
      }

      if (!matched) {
        noMatch++;
      }
    }

    // ====== BATCH UPDATE ======
    if (updates.length > 0) {
      const values = updates.map(u =>
        `('${u.id}'::uuid, ${u.street_id || 'NULL'}, ${u.geo_id || 'NULL'}, ${u.complex_id || 'NULL'})`
      ).join(',\n');

      await client.query(`
        UPDATE unified_listings ul
        SET
          street_id = COALESCE(v.street_id, ul.street_id),
          geo_id = COALESCE(v.geo_id, ul.geo_id),
          complex_id = COALESCE(v.complex_id, ul.complex_id)
        FROM (VALUES ${values}) AS v(id, street_id, geo_id, complex_id)
        WHERE ul.id = v.id
      `);
    }

    processed += listings.rows.length;
    offset += batchSize;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const rate = Math.round(processed / (parseFloat(elapsed) || 1));
    console.log(`Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | Street: ${streetMatches} | Complex: ${complexMatches} | NoMatch: ${noMatch} | ${elapsed}s (${rate}/s)`);
  }

  // ====== RESULTS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n=== Completed in ${totalTime}s ===`);
  console.log(`Street text matches: ${streetMatches} (${((streetMatches/totalCount)*100).toFixed(1)}%)`);
  console.log(`Complex text matches: ${complexMatches} (${((complexMatches/totalCount)*100).toFixed(1)}%)`);
  console.log(`Total text matches: ${streetMatches + complexMatches} (${(((streetMatches + complexMatches)/totalCount)*100).toFixed(1)}%)`);
  console.log(`No text match: ${noMatch} (${((noMatch/totalCount)*100).toFixed(1)}%)`);

  // Final coverage check
  const coverage = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(complex_id) as with_complex,
      COUNT(geo_id) as with_geo
    FROM unified_listings WHERE realty_platform = 'olx'
  `);

  const c = coverage.rows[0];
  console.log(`\n=== Final OLX Coverage ===`);
  console.log(`Total: ${c.total}`);
  console.log(`With street_id: ${c.with_street} (${((c.with_street/c.total)*100).toFixed(1)}%)`);
  console.log(`With complex_id: ${c.with_complex} (${((c.with_complex/c.total)*100).toFixed(1)}%)`);
  console.log(`With geo_id: ${c.with_geo} (${((c.with_geo/c.total)*100).toFixed(1)}%)`);

  await client.end();
}

main().catch(console.error);
