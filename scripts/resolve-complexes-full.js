const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

// ========== TEXT MATCHING (for OLX) ==========

function cleanName(name) {
  return name
    .replace(/^(жк|жилой комплекс|житловий комплекс|кг|км|котеджне|коттеджное|містечко|городок|таунхаус[иі]?|дуплекс[иі]?)\s*/gi, '')
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

    patterns.push(new RegExp(
      `(?:жк|жилой комплекс|житловий комплекс|кг|км)?\\s*["«']?${escapeRegex(cleaned)}["»']?`,
      'gi'
    ));

    if (cleaned.length >= 4) {
      patterns.push(new RegExp(`\\b${escapeRegex(cleaned)}\\b`, 'gi'));
    }
  }

  return patterns;
}

function findComplexInText(text, complexes) {
  const lowerText = text.toLowerCase();
  let bestMatch = null;
  let bestScore = 0;

  for (const complex of complexes) {
    for (const pattern of complex.patterns) {
      const match = lowerText.match(pattern);
      if (match) {
        const matchedText = match[0];
        const cleanedMatch = cleanName(matchedText).toLowerCase();
        const score = Math.min(cleanedMatch.length / complex.nameNormalized.length, 1);

        // Bonus for "жк" prefix
        const matchIndex = lowerText.indexOf(cleanedMatch);
        let finalScore = score * 0.6;
        if (matchIndex > 0) {
          const before = lowerText.substring(Math.max(0, matchIndex - 20), matchIndex);
          if (/жк\s*$/i.test(before)) finalScore += 0.3;
        }
        // Bonus for match in first 100 chars
        if (matchIndex < 100) finalScore += 0.1;

        if (finalScore > bestScore) {
          bestScore = finalScore;
          bestMatch = { complexId: complex.id, score: finalScore };
        }
      }
    }
  }

  return bestScore >= 0.5 ? bestMatch : null;
}

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to database\n');

  // Load complexes for text matching
  console.log('Loading complexes...');
  const complexRows = await client.query(`
    SELECT id, name_ru, name_uk, name_en, name_normalized
    FROM apartment_complexes
    ORDER BY LENGTH(name_normalized) DESC
  `);

  const complexes = complexRows.rows.map(r => ({
    id: r.id,
    nameRu: r.name_ru,
    nameNormalized: r.name_normalized,
    patterns: buildSearchPatterns(r.name_ru, r.name_uk, r.name_en)
  }));
  console.log(`Loaded ${complexes.length} complexes\n`);

  // ========== STEP 1: Coordinate matching for ALL platforms ==========
  console.log('=== STEP 1: Coordinate matching (point in polygon) ===\n');

  const coordUpdate = await client.query(`
    UPDATE unified_listings ul
    SET complex_id = ac.id
    FROM apartment_complexes ac
    WHERE ul.complex_id IS NULL
      AND ul.lat IS NOT NULL
      AND ul.lng IS NOT NULL
      AND ac.polygon IS NOT NULL
      AND ST_Contains(ac.polygon, ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326))
  `);
  console.log(`Matched by coordinates (in polygon): ${coordUpdate.rowCount}\n`);

  // ========== STEP 2: Nearest complex within 50m ==========
  console.log('=== STEP 2: Nearest complex within 50m ===\n');

  // This is slower, do in batches
  const nearestUpdate = await client.query(`
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
        AND ul.lat IS NOT NULL
        AND ul.lng IS NOT NULL
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
  console.log(`Matched by nearest (within 50m): ${nearestUpdate.rowCount}\n`);

  // ========== STEP 3: Text matching for OLX ==========
  console.log('=== STEP 3: Text matching for OLX ===\n');

  const BATCH_SIZE = 10000;
  let processed = 0;
  let matched = 0;
  let offset = 0;

  // Count OLX without complex
  const olxCount = await client.query(`
    SELECT COUNT(*) FROM unified_listings
    WHERE realty_platform = 'olx' AND complex_id IS NULL
  `);
  const totalOlx = parseInt(olxCount.rows[0].count);
  console.log(`OLX listings without complex: ${totalOlx}\n`);

  while (true) {
    const batch = await client.query(`
      SELECT id, description
      FROM unified_listings
      WHERE realty_platform = 'olx'
        AND complex_id IS NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [BATCH_SIZE, offset]);

    if (batch.rows.length === 0) break;

    const updates = [];

    for (const row of batch.rows) {
      processed++;

      let searchText = '';
      if (row.description) {
        try {
          const desc = typeof row.description === 'string'
            ? JSON.parse(row.description)
            : row.description;
          searchText = [desc.uk, desc.ru, desc.en].filter(Boolean).join(' ');
        } catch {
          searchText = String(row.description);
        }
      }

      if (!searchText || searchText.length < 10) continue;

      const match = findComplexInText(searchText, complexes);
      if (match) {
        updates.push({ id: row.id, complexId: match.complexId });
        matched++;
      }
    }

    // Batch update
    if (updates.length > 0) {
      const values = updates.map((u, i) => `($${i*2+1}::uuid, $${i*2+2}::int)`).join(',');
      const params = updates.flatMap(u => [u.id, u.complexId]);

      await client.query(`
        UPDATE unified_listings ul
        SET complex_id = v.complex_id
        FROM (VALUES ${values}) AS v(id, complex_id)
        WHERE ul.id = v.id
      `, params);
    }

    offset += BATCH_SIZE;

    if (processed % 50000 === 0 || batch.rows.length < BATCH_SIZE) {
      const pct = ((processed / totalOlx) * 100).toFixed(1);
      console.log(`Processed: ${processed}/${totalOlx} (${pct}%) | Matched: ${matched}`);
    }
  }

  console.log(`\nOLX text matching done: ${matched} matched\n`);

  // ========== FINAL STATS ==========
  console.log('=== FINAL STATISTICS ===\n');

  const stats = await client.query(`
    SELECT realty_platform,
           COUNT(*) as total,
           COUNT(complex_id) as with_complex
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY total DESC
  `);

  stats.rows.forEach(r => {
    const pct = ((parseInt(r.with_complex) / parseInt(r.total)) * 100).toFixed(1);
    console.log(`${r.realty_platform}: ${r.with_complex} / ${r.total} (${pct}%)`);
  });

  await client.end();
}

main().catch(console.error);
