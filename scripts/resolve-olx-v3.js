/**
 * OLX Street Resolution v3
 *
 * Логика (ТОЛЬКО для OLX):
 * 1. Сначала ищем ЖК в тексте описания
 *    - Если найден → берём street_id и geo_id из ЖК
 * 2. Если ЖК не найден → ищем улицу в тексте
 *    - Если найдена → берём street_id и geo_id из улицы
 * 3. Если ничего не найдено → оставляем как есть
 *    - НЕ используем coordinate fallback!
 *
 * Для других площадок эта логика НЕ применяется.
 */

const { Client } = require('pg');

// ====== COMPLEX MATCHING ======

function normalizeComplexName(name) {
  if (!name) return '';
  return name
    .replace(/^(жк|житловий комплекс|жилой комплекс|кг|км|котеджне містечко|коттеджный городок|апарт.комплекс|апарт.готель|клубний будинок|клубный дом|таунхаус[иі]?|дуплекс[иі]?)\s*/gi, '')
    .replace(/[«»""''`'()]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

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

function findInText(normalizedName, text) {
  if (!normalizedName || normalizedName.length < 3) return false;
  if (!text) return false;

  // Direct substring match
  if (text.includes(normalizedName)) return true;

  // Word boundary match
  const escaped = normalizedName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(^|\\s|,|\\.|:|-)${escaped}($|\\s|,|\\.|:|-|\\d)`, 'i');
  return regex.test(text);
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX Street Resolution v3 ===');
  console.log('Логика: ЖК → Улица → (без coordinate fallback)\n');

  // ====== LOAD COMPLEXES (with street_id) ======
  console.log('Loading complexes...');
  const complexesResult = await client.query(`
    SELECT id, name_ru, name_uk, street_id, geo_id
    FROM apartment_complexes
    WHERE street_id IS NOT NULL
  `);

  const complexes = [];
  const complexNameMap = new Map(); // normalized name -> complex

  for (const c of complexesResult.rows) {
    const complex = {
      id: c.id,
      nameRu: c.name_ru,
      nameUk: c.name_uk,
      street_id: c.street_id,
      geo_id: c.geo_id
    };
    complexes.push(complex);

    const normRu = normalizeComplexName(c.name_ru);
    const normUk = normalizeComplexName(c.name_uk);

    if (normRu && normRu.length >= 3) {
      complexNameMap.set(normRu, complex);
    }
    if (normUk && normUk.length >= 3 && normUk !== normRu) {
      complexNameMap.set(normUk, complex);
    }
  }
  console.log(`Loaded ${complexes.length} complexes (${complexNameMap.size} unique names)`);

  // Build complex search pattern (sorted by length desc for greedy matching)
  const sortedComplexNames = [...complexNameMap.keys()].sort((a, b) => b.length - a.length);
  const escapedComplexNames = sortedComplexNames.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const complexPattern = new RegExp(
    `(?:жк|житловий комплекс|жилой комплекс|кг|км|апарт.комплекс|клубний будинок|клубный дом)\\s*["«']?(${escapedComplexNames.join('|')})["»']?`,
    'gi'
  );

  // ====== LOAD STREETS ======
  console.log('Loading streets...');
  const streetsResult = await client.query(`
    SELECT id, geo_id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM streets
    WHERE geo_id IS NOT NULL
  `);

  const streets = [];
  const streetNameMap = new Map(); // normalized name -> street

  for (const s of streetsResult.rows) {
    const street = {
      id: s.id,
      geo_id: s.geo_id,
      nameUk: s.name_uk,
      nameRu: s.name_ru,
      normUk: normalizeStreetName(s.name_uk),
      normRu: normalizeStreetName(s.name_ru)
    };
    streets.push(street);

    if (street.normUk && street.normUk.length >= 3) {
      streetNameMap.set(street.normUk, street);
    }
    if (street.normRu && street.normRu.length >= 3 && street.normRu !== street.normUk) {
      streetNameMap.set(street.normRu, street);
    }
  }
  console.log(`Loaded ${streets.length} streets (${streetNameMap.size} unique names)\n`);

  // Build street search patterns (sorted by length desc)
  const sortedStreetNames = [...streetNameMap.keys()].sort((a, b) => b.length - a.length);

  // ====== COUNT OLX LISTINGS ======
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings
    WHERE realty_platform = 'olx'
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OLX listings: ${totalCount}\n`);

  // ====== PROCESS ======
  let processed = 0;
  let complexMatches = 0;
  let streetMatches = 0;
  let noMatch = 0;
  let offset = 0;
  const batchSize = 500;
  const startTime = Date.now();

  while (processed < totalCount) {
    const listings = await client.query(`
      SELECT id, description->>'uk' as desc_uk, description->>'ru' as desc_ru
      FROM unified_listings
      WHERE realty_platform = 'olx'
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (listings.rows.length === 0) break;

    const updates = [];

    for (const listing of listings.rows) {
      const descText = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).toLowerCase();

      if (!descText.trim()) {
        noMatch++;
        continue;
      }

      let matched = false;

      // ====== STEP 1: Search for COMPLEX in text (priority!) ======
      complexPattern.lastIndex = 0;
      const complexMatch = complexPattern.exec(descText);

      if (complexMatch) {
        const matchedName = complexMatch[1].toLowerCase();
        const complex = complexNameMap.get(matchedName);

        if (complex && complex.street_id) {
          updates.push({
            id: listing.id,
            street_id: complex.street_id,
            geo_id: complex.geo_id,
            complex_id: complex.id
          });
          complexMatches++;
          matched = true;
        }
      }

      // ====== STEP 2: If no complex, search for STREET in text ======
      if (!matched) {
        // Search for street names in text
        for (const streetName of sortedStreetNames) {
          if (findInText(streetName, descText)) {
            const street = streetNameMap.get(streetName);
            if (street) {
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
        }
      }

      // ====== STEP 3: No match - leave as is (NO coordinate fallback!) ======
      if (!matched) {
        noMatch++;
      }
    }

    // ====== BATCH UPDATE ======
    if (updates.length > 0) {
      const values = updates.map(u =>
        `('${u.id}'::uuid, ${u.street_id}, ${u.geo_id || 'NULL'}, ${u.complex_id || 'NULL'})`
      ).join(',\n');

      await client.query(`
        UPDATE unified_listings ul
        SET
          street_id = v.street_id,
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
    console.log(`Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | Complex: ${complexMatches} | Street: ${streetMatches} | NoMatch: ${noMatch} | ${elapsed}s (${rate}/s)`);
  }

  // ====== RESULTS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n=== Completed in ${totalTime}s ===`);
  console.log(`Complex text matches: ${complexMatches} (${((complexMatches/totalCount)*100).toFixed(1)}%)`);
  console.log(`Street text matches: ${streetMatches} (${((streetMatches/totalCount)*100).toFixed(1)}%)`);
  console.log(`Total text matches: ${complexMatches + streetMatches} (${(((complexMatches + streetMatches)/totalCount)*100).toFixed(1)}%)`);
  console.log(`No text match (unchanged): ${noMatch} (${((noMatch/totalCount)*100).toFixed(1)}%)`);

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
