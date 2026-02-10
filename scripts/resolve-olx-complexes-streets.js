/**
 * Bulk script: Resolve complex_id and street_id for OLX apartment listings
 *
 * Strategy:
 *   Phase 1: Complex matching
 *     Step 1: OLX zkh param → text match against apartment_complexes
 *     Step 2: OLX title → text match against apartment_complexes
 *     Step 3: OLX description → text match against apartment_complexes
 *     Step 4: Non-OLX → coordinate matching (ST_Contains + nearest 50m)
 *
 *   Phase 2: Street resolution
 *     Step 5: Listings with complex_id → derive street from complex coordinates
 *     Step 6: OLX without complex → text-based street matching (no nearest fallback)
 *
 * Usage: node scripts/resolve-olx-complexes-streets.js
 */

const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres',
};

const BATCH_SIZE = 5000;

// ========== Complex Text Matching ==========

function cleanName(name) {
  return name
    .replace(
      /^(жк|жилой комплекс|житловий комплекс|кг|км|котеджне|коттеджное|містечко|городок|таунхаус[иі]?|дуплекс[иі]?)\s*/gi,
      '',
    )
    .replace(/["«»'']/g, '')
    .replace(/\s*\([^)]+\)\s*/g, ' ')
    .replace(/\s*буд\.?\s*\d+/gi, '')
    .trim();
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildSearchPatterns(nameRu, nameUk, nameEn) {
  const patterns = [];
  const names = [nameRu, nameUk, nameEn].filter(Boolean);

  for (const name of names) {
    const cleaned = cleanName(name);
    if (cleaned.length < 3) continue;

    // Pattern with ЖК prefix
    patterns.push(
      new RegExp(
        `(?:жк|жилой комплекс|житловий комплекс|кг|км)?\\s*["«']?${escapeRegex(cleaned)}["»']?`,
        'gi',
      ),
    );

    // Exact word match
    if (cleaned.length >= 4) {
      patterns.push(new RegExp(`\\b${escapeRegex(cleaned)}\\b`, 'gi'));
    }

    // Multi-word: match any 2 consecutive words
    const words = cleaned.split(/\s+/).filter((w) => w.length >= 3);
    if (words.length >= 2) {
      for (let i = 0; i < words.length - 1; i++) {
        patterns.push(new RegExp(`\\b${escapeRegex(words[i])}\\s+${escapeRegex(words[i + 1])}\\b`, 'gi'));
      }
    }
  }

  return patterns;
}

function calculateMatchScore(matchedText, normalizedName, fullText) {
  const cleanedMatch = cleanName(matchedText).toLowerCase();
  const nameLength = normalizedName.length;
  const matchLength = cleanedMatch.length;

  let score = Math.min(matchLength / nameLength, 1) * 0.6;

  // Bonus for match in first 100 chars (likely title)
  const titlePart = fullText.substring(0, 100);
  if (titlePart.includes(cleanedMatch)) {
    score += 0.2;
  }

  // Bonus for "жк" prefix near match
  const matchIndex = fullText.indexOf(cleanedMatch);
  if (matchIndex > 0) {
    const before = fullText.substring(Math.max(0, matchIndex - 30), matchIndex);
    if (/(?:жк|жилой комплекс|житловий комплекс)\s*$/i.test(before)) {
      score += 0.2;
    }
  }

  return Math.min(score, 1);
}

function findComplexInText(text, complexes, minScore = 0.5) {
  const lowerText = text.toLowerCase();
  if (!lowerText || lowerText.length < 3) return null;

  let bestMatch = null;
  let bestScore = 0;

  for (const complex of complexes) {
    for (const pattern of complex.patterns) {
      pattern.lastIndex = 0; // Reset regex state
      const match = lowerText.match(pattern);
      if (match) {
        const score = calculateMatchScore(match[0], complex.nameNormalized, lowerText);

        if (score > bestScore) {
          bestScore = score;
          bestMatch = { complexId: complex.id, complexName: complex.nameRu, score };
        }
      }
    }
  }

  return bestScore >= minScore ? bestMatch : null;
}

// ========== Street Text Matching ==========

const STREET_PATTERNS = [
  // Українська
  { regex: /(?:вул(?:иця)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'вулиця' },
  { regex: /(?:просп(?:ект)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'проспект' },
  { regex: /(?:пров(?:улок)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'провулок' },
  { regex: /(?:бульв(?:ар)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'бульвар' },
  { regex: /(?:пл(?:оща)?\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'площа' },
  { regex: /(?:набережна\.?\s+)([А-ЯІЇЄҐа-яіїєґ][А-ЯІЇЄҐа-яіїєґ\s\-']+)/gi, type: 'набережна' },
  // Російська
  { regex: /(?:ул(?:ица)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'улица' },
  { regex: /(?:пр(?:оспект)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'проспект' },
  { regex: /(?:пер(?:еулок)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'переулок' },
  { regex: /(?:бульв(?:ар)?\.?\s+)([А-Яа-яЁё][А-Яа-яЁё\s\-]+)/gi, type: 'бульвар' },
];

function extractStreetFromText(text) {
  if (!text) return null;

  for (const pattern of STREET_PATTERNS) {
    pattern.regex.lastIndex = 0;
    const matches = text.matchAll(pattern.regex);
    for (const match of matches) {
      if (match[1]) {
        const name = match[1]
          .replace(/\s*,.*$/, '')
          .replace(/\s*\d+.*$/, '')
          .trim();
        if (name.length >= 3) {
          return { name, type: pattern.type };
        }
      }
    }
  }

  return null;
}

function normalizeStreetName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/[''`]/g, "'")
    .replace(/\s+/g, ' ')
    .replace(/\d+.*$/, '')
    .trim();
}

function levenshteinSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  if (str1 === str2) return 1;

  const len1 = str1.length;
  const len2 = str2.length;

  if (Math.abs(len1 - len2) > Math.max(len1, len2) * 0.5) return 0;
  if (str1.includes(str2) || str2.includes(str1)) return 0.9;

  const matrix = [];
  for (let i = 0; i <= len1; i++) matrix[i] = [i];
  for (let j = 0; j <= len2; j++) matrix[0][j] = j;

  for (let i = 1; i <= len1; i++) {
    for (let j = 1; j <= len2; j++) {
      const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost);
    }
  }

  return 1 - matrix[len1][len2] / Math.max(len1, len2);
}

// ========== Main Script ==========

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to database\n');

  // ========== Load complexes ==========
  console.log('Loading apartment complexes...');
  const complexRows = await client.query(`
    SELECT id, name_ru, name_uk, name_en, name_normalized, lat, lng,
           geo_id, street_id, polygon IS NOT NULL as has_polygon
    FROM apartment_complexes
    WHERE name_normalized IS NOT NULL AND LENGTH(name_normalized) >= 3
    ORDER BY LENGTH(name_normalized) DESC
  `);

  const complexes = complexRows.rows.map((r) => ({
    id: r.id,
    nameRu: r.name_ru,
    nameUk: r.name_uk,
    nameNormalized: r.name_normalized,
    lat: parseFloat(r.lat),
    lng: parseFloat(r.lng),
    geoId: r.geo_id,
    streetId: r.street_id,
    hasPolygon: r.has_polygon,
    patterns: buildSearchPatterns(r.name_ru, r.name_uk, r.name_en),
  }));
  console.log(`Loaded ${complexes.length} complexes\n`);

  // ========== Step 0: Reset complex_id = 0 to NULL ==========
  console.log('=== Step 0: Reset complex_id = 0 to NULL ===');
  const resetResult = await client.query(`
    UPDATE unified_listings SET complex_id = NULL WHERE complex_id = 0
  `);
  console.log(`Reset ${resetResult.rowCount} rows with complex_id=0 to NULL\n`);

  // ========== PHASE 1: Complex matching for OLX apartments ==========
  console.log('========== PHASE 1: Complex matching ==========\n');

  // Step 1: OLX zkh param matching
  console.log('=== Step 1: OLX zkh param matching ===');
  await matchOlxByZkh(client, complexes);

  // Step 2: OLX title matching
  console.log('\n=== Step 2: OLX title/description matching (remaining) ===');
  await matchOlxByTitleDescription(client, complexes);

  // Step 3: Non-OLX coordinate matching
  console.log('\n=== Step 3: Non-OLX coordinate matching (ST_Contains) ===');
  const coordResult = await client.query(`
    UPDATE unified_listings ul
    SET complex_id = ac.id
    FROM apartment_complexes ac
    WHERE ul.complex_id IS NULL
      AND ul.realty_platform != 'olx'
      AND ul.lat IS NOT NULL AND ul.lng IS NOT NULL
      AND ac.polygon IS NOT NULL
      AND ST_Contains(ac.polygon, ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326))
  `);
  console.log(`Matched by ST_Contains: ${coordResult.rowCount}`);

  // Step 4: Non-OLX nearest 50m
  console.log('\n=== Step 4: Non-OLX nearest complex within 50m ===');
  const nearestResult = await client.query(`
    WITH nearest AS (
      SELECT DISTINCT ON (ul.id)
        ul.id as listing_id,
        ac.id as complex_id
      FROM unified_listings ul
      JOIN apartment_complexes ac ON ST_DWithin(
        ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326)::geography,
        ST_SetSRID(ST_MakePoint(ac.lng, ac.lat), 4326)::geography,
        50
      )
      WHERE ul.complex_id IS NULL
        AND ul.realty_platform != 'olx'
        AND ul.lat IS NOT NULL AND ul.lng IS NOT NULL
      ORDER BY ul.id, ST_Distance(
        ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326)::geography,
        ST_SetSRID(ST_MakePoint(ac.lng, ac.lat), 4326)::geography
      )
    )
    UPDATE unified_listings ul
    SET complex_id = n.complex_id
    FROM nearest n
    WHERE ul.id = n.listing_id
  `);
  console.log(`Matched by nearest 50m: ${nearestResult.rowCount}`);

  // ========== PHASE 2: Street resolution ==========
  console.log('\n========== PHASE 2: Street resolution ==========\n');

  // Step 5: Derive street from complex coordinates
  console.log('=== Step 5: Derive street from complex coordinates ===');
  await deriveStreetFromComplex(client);

  // Step 6: OLX text-based street matching for remaining
  console.log('\n=== Step 6: OLX text-based street matching ===');
  await matchOlxStreetsByText(client);

  // ========== FINAL STATS ==========
  console.log('\n========== FINAL STATISTICS ==========\n');
  await printStats(client);

  await client.end();
  console.log('\nDone!');
}

// ========== Step 1: Match OLX by zkh param ==========
async function matchOlxByZkh(client, complexes) {
  let processed = 0;
  let matched = 0;
  let offset = 0;

  const countResult = await client.query(`
    SELECT COUNT(*) FROM unified_listings
    WHERE realty_platform = 'olx'
      AND realty_type = 'apartment'
      AND complex_id IS NULL
      AND primary_data IS NOT NULL
      AND primary_data->'params' IS NOT NULL
  `);
  const total = parseInt(countResult.rows[0].count);
  console.log(`OLX apartments with primaryData params: ${total}`);

  while (true) {
    const batch = await client.query(
      `
      SELECT id, primary_data
      FROM unified_listings
      WHERE realty_platform = 'olx'
        AND realty_type = 'apartment'
        AND complex_id IS NULL
        AND primary_data IS NOT NULL
        AND primary_data->'params' IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `,
      [BATCH_SIZE, offset],
    );

    if (batch.rows.length === 0) break;

    const updates = [];

    for (const row of batch.rows) {
      processed++;

      // Extract zkh param
      const params = row.primary_data?.params;
      if (!Array.isArray(params)) continue;

      const zkhParam = params.find((p) => p.key === 'zkh' || p.key === 'complex_name');
      if (!zkhParam?.value) continue;

      const zkhValue = String(zkhParam.value).trim();
      if (zkhValue.length < 3) continue;

      // Search for this zkh value in complexes
      const match = findComplexInText(zkhValue, complexes, 0.4); // Lower threshold for zkh - it's explicit
      if (match) {
        updates.push({ id: row.id, complexId: match.complexId });
        matched++;
      }
    }

    // Batch update
    if (updates.length > 0) {
      const values = updates.map((u, i) => `($${i * 2 + 1}::uuid, $${i * 2 + 2}::int)`).join(',');
      const params = updates.flatMap((u) => [u.id, u.complexId]);

      await client.query(
        `
        UPDATE unified_listings ul
        SET complex_id = v.complex_id
        FROM (VALUES ${values}) AS v(id, complex_id)
        WHERE ul.id = v.id
      `,
        params,
      );
    }

    offset += BATCH_SIZE;

    if (processed % 25000 === 0 || batch.rows.length < BATCH_SIZE) {
      console.log(`  Processed: ${processed}/${total} | Matched by zkh: ${matched}`);
    }
  }

  console.log(`Zkh matching done: ${matched} matched out of ${processed} processed`);
}

// ========== Step 2: Match OLX by title/description ==========
async function matchOlxByTitleDescription(client, complexes) {
  let processed = 0;
  let matched = 0;
  let offset = 0;

  const countResult = await client.query(`
    SELECT COUNT(*) FROM unified_listings
    WHERE realty_platform = 'olx'
      AND realty_type = 'apartment'
      AND complex_id IS NULL
  `);
  const total = parseInt(countResult.rows[0].count);
  console.log(`OLX apartments still without complex: ${total}`);

  while (true) {
    const batch = await client.query(
      `
      SELECT id, primary_data, description
      FROM unified_listings
      WHERE realty_platform = 'olx'
        AND realty_type = 'apartment'
        AND complex_id IS NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `,
      [BATCH_SIZE, offset],
    );

    if (batch.rows.length === 0) break;

    const updates = [];

    for (const row of batch.rows) {
      processed++;

      // Build search text from title + description
      const parts = [];

      // Title from primaryData
      if (row.primary_data?.title) {
        parts.push(String(row.primary_data.title));
      }

      // Description
      if (row.description) {
        const desc = row.description;
        if (desc.uk) parts.push(desc.uk);
        if (desc.ru) parts.push(desc.ru);
      }

      const searchText = parts.join(' ');
      if (searchText.length < 10) continue;

      const match = findComplexInText(searchText, complexes, 0.5);
      if (match) {
        updates.push({ id: row.id, complexId: match.complexId });
        matched++;
      }
    }

    // Batch update
    if (updates.length > 0) {
      const values = updates.map((u, i) => `($${i * 2 + 1}::uuid, $${i * 2 + 2}::int)`).join(',');
      const params = updates.flatMap((u) => [u.id, u.complexId]);

      await client.query(
        `
        UPDATE unified_listings ul
        SET complex_id = v.complex_id
        FROM (VALUES ${values}) AS v(id, complex_id)
        WHERE ul.id = v.id
      `,
        params,
      );
    }

    offset += BATCH_SIZE;

    if (processed % 25000 === 0 || batch.rows.length < BATCH_SIZE) {
      console.log(`  Processed: ${processed}/${total} | Matched by title/desc: ${matched}`);
    }
  }

  console.log(`Title/description matching done: ${matched} matched out of ${processed} processed`);
}

// ========== Step 5: Derive street from complex coordinates ==========
async function deriveStreetFromComplex(client) {
  // For each complex that has coordinates, find the nearest street
  // Then update all listings with that complex_id
  const result = await client.query(`
    WITH complex_nearest_street AS (
      SELECT DISTINCT ON (ac.id)
        ac.id as complex_id,
        s.id as street_id
      FROM apartment_complexes ac
      JOIN streets s ON s.line IS NOT NULL
        AND ST_DWithin(
          s.line::geography,
          ST_SetSRID(ST_MakePoint(ac.lng, ac.lat), 4326)::geography,
          300
        )
      WHERE ac.lat IS NOT NULL AND ac.lng IS NOT NULL
      ORDER BY ac.id, ST_Distance(
        s.line::geography,
        ST_SetSRID(ST_MakePoint(ac.lng, ac.lat), 4326)::geography
      )
    )
    UPDATE unified_listings ul
    SET street_id = cns.street_id
    FROM complex_nearest_street cns
    WHERE ul.complex_id = cns.complex_id
      AND ul.complex_id IS NOT NULL
      AND (ul.street_id IS NULL OR ul.realty_platform = 'olx')
  `);

  console.log(`Updated street_id from complex coordinates: ${result.rowCount} listings`);
}

// ========== Step 6: OLX text-based street matching ==========
async function matchOlxStreetsByText(client) {
  // Load streets indexed by geo_id for fast lookup
  console.log('Loading streets...');
  const streetRows = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, geo_id
    FROM streets
    WHERE name->>'uk' IS NOT NULL AND LENGTH(name->>'uk') >= 3
  `);

  // Index streets by geo_id
  const streetsByGeo = new Map();
  for (const row of streetRows.rows) {
    const geoId = row.geo_id;
    if (!streetsByGeo.has(geoId)) {
      streetsByGeo.set(geoId, []);
    }
    streetsByGeo.get(geoId).push({
      id: row.id,
      nameUk: row.name_uk,
      nameRu: row.name_ru,
      nameUkNorm: normalizeStreetName(row.name_uk),
      nameRuNorm: row.name_ru ? normalizeStreetName(row.name_ru) : null,
    });
  }
  console.log(`Loaded ${streetRows.rows.length} streets across ${streetsByGeo.size} geos`);

  // Process OLX apartments without street_id (or with unreliable street_id) that have geo_id
  let processed = 0;
  let matched = 0;
  let cleared = 0;
  let offset = 0;

  const countResult = await client.query(`
    SELECT COUNT(*) FROM unified_listings
    WHERE realty_platform = 'olx'
      AND realty_type = 'apartment'
      AND complex_id IS NULL
      AND geo_id IS NOT NULL
  `);
  const total = parseInt(countResult.rows[0].count);
  console.log(`OLX apartments without complex, with geo_id: ${total}`);

  while (true) {
    const batch = await client.query(
      `
      SELECT id, geo_id, street_id, primary_data, description
      FROM unified_listings
      WHERE realty_platform = 'olx'
        AND realty_type = 'apartment'
        AND complex_id IS NULL
        AND geo_id IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `,
      [BATCH_SIZE, offset],
    );

    if (batch.rows.length === 0) break;

    const streetUpdates = [];
    const streetClears = [];

    for (const row of batch.rows) {
      processed++;

      // Build search text
      const parts = [];
      if (row.primary_data?.title) parts.push(String(row.primary_data.title));
      if (row.description?.uk) parts.push(row.description.uk);
      if (row.description?.ru) parts.push(row.description.ru);

      // Also try location.pathName from OLX
      if (row.primary_data?.location?.pathName) {
        parts.push(String(row.primary_data.location.pathName));
      }

      // domRia street name
      if (row.primary_data?.street_name_uk) parts.push(String(row.primary_data.street_name_uk));
      if (row.primary_data?.street_name) parts.push(String(row.primary_data.street_name));

      const searchText = parts.join(' ');

      // Try to extract street name from text
      const parsed = extractStreetFromText(searchText);
      if (parsed) {
        // Find matching street in this geo
        const geoStreets = streetsByGeo.get(row.geo_id) || [];
        const normalizedParsed = normalizeStreetName(parsed.name);

        let bestStreet = null;
        let bestScore = 0;

        for (const street of geoStreets) {
          const scoreUk = levenshteinSimilarity(normalizedParsed, street.nameUkNorm);
          const scoreRu = street.nameRuNorm ? levenshteinSimilarity(normalizedParsed, street.nameRuNorm) : 0;
          const score = Math.max(scoreUk, scoreRu);

          if (score > bestScore) {
            bestScore = score;
            bestStreet = street;
          }
        }

        if (bestStreet && bestScore >= 0.7) {
          if (row.street_id !== bestStreet.id) {
            streetUpdates.push({ id: row.id, streetId: bestStreet.id });
          }
          matched++;
          continue;
        }
      }

      // Also try: find any geo street name mentioned in text
      if (searchText.length > 10) {
        const geoStreets = streetsByGeo.get(row.geo_id) || [];
        const lowerText = searchText.toLowerCase();
        let found = false;

        for (const street of geoStreets) {
          if (street.nameUkNorm.length < 4) continue;
          if (lowerText.includes(street.nameUkNorm)) {
            if (row.street_id !== street.id) {
              streetUpdates.push({ id: row.id, streetId: street.id });
            }
            matched++;
            found = true;
            break;
          }
          if (street.nameRuNorm && street.nameRuNorm.length >= 4 && lowerText.includes(street.nameRuNorm)) {
            if (row.street_id !== street.id) {
              streetUpdates.push({ id: row.id, streetId: street.id });
            }
            matched++;
            found = true;
            break;
          }
        }

        if (found) continue;
      }

      // No text match → if street_id was set by coordinates, clear it (unreliable for OLX)
      if (row.street_id) {
        streetClears.push(row.id);
        cleared++;
      }
    }

    // Batch update streets
    if (streetUpdates.length > 0) {
      const values = streetUpdates.map((u, i) => `($${i * 2 + 1}::uuid, $${i * 2 + 2}::int)`).join(',');
      const params = streetUpdates.flatMap((u) => [u.id, u.streetId]);

      await client.query(
        `
        UPDATE unified_listings ul
        SET street_id = v.street_id
        FROM (VALUES ${values}) AS v(id, street_id)
        WHERE ul.id = v.id
      `,
        params,
      );
    }

    // Batch clear unreliable streets
    if (streetClears.length > 0) {
      // Do in chunks to avoid parameter limit
      for (let i = 0; i < streetClears.length; i += 1000) {
        const chunk = streetClears.slice(i, i + 1000);
        const placeholders = chunk.map((_, idx) => `$${idx + 1}::uuid`).join(',');
        await client.query(
          `UPDATE unified_listings SET street_id = NULL WHERE id IN (${placeholders})`,
          chunk,
        );
      }
    }

    offset += BATCH_SIZE;

    if (processed % 25000 === 0 || batch.rows.length < BATCH_SIZE) {
      console.log(
        `  Processed: ${processed}/${total} | Street matched: ${matched} | Cleared: ${cleared}`,
      );
    }
  }

  console.log(`Street text matching done: ${matched} matched, ${cleared} cleared`);
}

// ========== Stats ==========
async function printStats(client) {
  // Complex stats
  const complexStats = await client.query(`
    SELECT
      realty_platform,
      realty_type,
      COUNT(*) as total,
      COUNT(complex_id) as with_complex,
      COUNT(street_id) as with_street,
      COUNT(*) FILTER (WHERE is_active) as active,
      COUNT(complex_id) FILTER (WHERE is_active) as active_with_complex,
      COUNT(street_id) FILTER (WHERE is_active) as active_with_street
    FROM unified_listings
    GROUP BY realty_platform, realty_type
    ORDER BY total DESC
  `);

  console.log('Per platform + type:');
  console.log(
    'Platform       | Type       | Total    | Complex  | Street   | Active   | Act+Cmplx| Act+Street',
  );
  console.log('-'.repeat(105));

  for (const r of complexStats.rows) {
    const platform = (r.realty_platform || 'null').padEnd(14);
    const type = (r.realty_type || 'null').padEnd(10);
    const total = String(r.total).padStart(8);
    const complex = String(r.with_complex).padStart(8);
    const street = String(r.with_street).padStart(8);
    const active = String(r.active).padStart(8);
    const activeComplex = String(r.active_with_complex).padStart(8);
    const activeStreet = String(r.active_with_street).padStart(8);
    console.log(
      `${platform} | ${type} | ${total} | ${complex} | ${street} | ${active} | ${activeComplex} | ${activeStreet}`,
    );
  }

  // OLX apartment summary
  const olxSummary = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(complex_id) as with_complex,
      COUNT(street_id) as with_street,
      COUNT(*) FILTER (WHERE complex_id IS NULL AND street_id IS NULL) as no_data,
      COUNT(*) FILTER (WHERE is_active) as active,
      COUNT(complex_id) FILTER (WHERE is_active) as active_complex,
      COUNT(street_id) FILTER (WHERE is_active) as active_street
    FROM unified_listings
    WHERE realty_platform = 'olx' AND realty_type = 'apartment'
  `);

  const s = olxSummary.rows[0];
  console.log('\nOLX Apartment Summary:');
  console.log(`  Total: ${s.total}`);
  console.log(`  With complex: ${s.with_complex} (${((s.with_complex / s.total) * 100).toFixed(1)}%)`);
  console.log(`  With street: ${s.with_street} (${((s.with_street / s.total) * 100).toFixed(1)}%)`);
  console.log(`  No data: ${s.no_data} (${((s.no_data / s.total) * 100).toFixed(1)}%)`);
  console.log(`  Active: ${s.active}`);
  console.log(`  Active with complex: ${s.active_complex}`);
  console.log(`  Active with street: ${s.active_street}`);
}

main().catch((err) => {
  console.error('Script failed:', err);
  process.exit(1);
});
