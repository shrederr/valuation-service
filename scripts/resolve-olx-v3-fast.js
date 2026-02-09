/**
 * OLX Street Resolution v3 (FAST)
 *
 * Логика (ТОЛЬКО для OLX):
 * 1. Сначала ищем ЖК в тексте описания
 * 2. Если ЖК не найден → ищем улицу в тексте
 * 3. Если ничего не найдено → оставляем как есть (NO coordinate fallback!)
 *
 * Оптимизация: используем regex patterns для быстрого поиска
 */

const { Client } = require('pg');

function normalizeComplexName(name) {
  if (!name) return '';
  return name
    .replace(/^(жк|житловий комплекс|жилой комплекс|кг|км|котеджне містечко|коттеджный городок|апарт.комплекс|апарт.готель|клубний будинок|клубный дом|таунхаус[иі]?|дуплекс[иі]?)\s*/gi, '')
    .replace(/[«»""''`'()]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

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

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== OLX Street Resolution v3 (FAST) ===');
  console.log('Логика: ЖК → Улица → (без coordinate fallback)\n');

  // ====== LOAD COMPLEXES ======
  console.log('Loading complexes...');
  const complexesResult = await client.query(`
    SELECT id, name_ru, name_uk, street_id, geo_id
    FROM apartment_complexes
    WHERE street_id IS NOT NULL
  `);

  const complexNameMap = new Map();
  for (const c of complexesResult.rows) {
    const complex = { id: c.id, street_id: c.street_id, geo_id: c.geo_id };
    const normRu = normalizeComplexName(c.name_ru);
    const normUk = normalizeComplexName(c.name_uk);
    if (normRu && normRu.length >= 3) complexNameMap.set(normRu, complex);
    if (normUk && normUk.length >= 3) complexNameMap.set(normUk, complex);
  }
  console.log(`Loaded ${complexNameMap.size} unique complex names`);

  // Build complex regex pattern
  const complexNames = [...complexNameMap.keys()].sort((a, b) => b.length - a.length);
  const complexPattern = new RegExp(
    `(?:жк|житловий комплекс|жилой комплекс|кг|км|апарт.комплекс|клубний будинок|клубный дом)\\s*["«']?(${complexNames.map(escapeRegex).join('|')})["»']?`,
    'gi'
  );

  // ====== LOAD STREETS ======
  console.log('Loading streets...');
  const streetsResult = await client.query(`
    SELECT id, geo_id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM streets WHERE geo_id IS NOT NULL
  `);

  const streetNameMap = new Map();
  for (const s of streetsResult.rows) {
    const street = { id: s.id, geo_id: s.geo_id };
    const normUk = normalizeStreetName(s.name_uk);
    const normRu = normalizeStreetName(s.name_ru);
    if (normUk && normUk.length >= 3) streetNameMap.set(normUk, street);
    if (normRu && normRu.length >= 3) streetNameMap.set(normRu, street);
  }
  console.log(`Loaded ${streetNameMap.size} unique street names`);

  // Build street regex pattern (split into chunks if too large)
  const streetNames = [...streetNameMap.keys()].sort((a, b) => b.length - a.length);
  console.log(`Building street pattern with ${streetNames.length} names...`);

  // Street prefixes to look for
  const streetPrefixes = '(?:вулиця|вул\\.|вул|улица|ул\\.|ул|проспект|просп\\.|пр-т|провулок|пров\\.|переулок|пер\\.|бульвар|бульв\\.|б-р|площа|пл\\.|набережна|наб\\.|шосе|шоссе|алея)';

  // Build pattern in chunks to avoid regex too large error
  const chunkSize = 5000;
  const streetPatterns = [];
  for (let i = 0; i < streetNames.length; i += chunkSize) {
    const chunk = streetNames.slice(i, i + chunkSize);
    const pattern = new RegExp(
      `${streetPrefixes}\\s+["«']?(${chunk.map(escapeRegex).join('|')})["»']?`,
      'gi'
    );
    streetPatterns.push(pattern);
  }
  console.log(`Created ${streetPatterns.length} street patterns\n`);

  // ====== COUNT OLX ======
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM unified_listings WHERE realty_platform = 'olx'
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OLX listings: ${totalCount}\n`);

  // ====== PROCESS ======
  let processed = 0;
  let complexMatches = 0;
  let streetMatches = 0;
  let noMatch = 0;
  let offset = 0;
  const batchSize = 1000;
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

      // ====== STEP 1: Search for COMPLEX ======
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

      // ====== STEP 2: Search for STREET ======
      if (!matched) {
        for (const pattern of streetPatterns) {
          pattern.lastIndex = 0;
          const streetMatch = pattern.exec(descText);

          if (streetMatch) {
            const matchedName = streetMatch[1].toLowerCase();
            const street = streetNameMap.get(matchedName);

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
  console.log(`No text match: ${noMatch} (${((noMatch/totalCount)*100).toFixed(1)}%)`);

  // Final coverage
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
