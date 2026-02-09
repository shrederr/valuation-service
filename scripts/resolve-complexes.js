const { Client } = require('pg');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

// Clean name for matching (remove ЖК prefixes)
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

    // Pattern with ЖК prefix
    patterns.push(new RegExp(
      `(?:жк|жилой комплекс|житловий комплекс|кг|км)?\\s*["«']?${escapeRegex(cleaned)}["»']?`,
      'gi'
    ));

    // Just the name
    if (cleaned.length >= 4) {
      patterns.push(new RegExp(`\\b${escapeRegex(cleaned)}\\b`, 'gi'));
    }
  }

  return patterns;
}

function calculateMatchScore(matchedText, normalizedName, fullText) {
  let score = 0;
  const cleanedMatch = cleanName(matchedText).toLowerCase();
  const nameLength = normalizedName.length;
  const matchLength = cleanedMatch.length;

  score = Math.min(matchLength / nameLength, 1) * 0.6;

  // Bonus for match in title (first 100 chars)
  const titlePart = fullText.substring(0, 100);
  if (titlePart.includes(cleanedMatch)) {
    score += 0.2;
  }

  // Bonus for "жк" prefix near match
  const matchIndex = fullText.indexOf(cleanedMatch);
  if (matchIndex > 0) {
    const beforeMatch = fullText.substring(Math.max(0, matchIndex - 30), matchIndex);
    if (/(?:жк|жилой комплекс|житловий комплекс)\s*$/i.test(beforeMatch)) {
      score += 0.2;
    }
  }

  return Math.min(score, 1);
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
        const score = calculateMatchScore(matchedText, complex.nameNormalized, lowerText);

        if (score > bestScore) {
          bestScore = score;
          bestMatch = {
            complexId: complex.id,
            complexName: complex.nameRu,
            matchedText,
            score
          };
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

  // Load complexes
  console.log('Loading complexes...');
  const complexRows = await client.query(`
    SELECT id, name_ru, name_uk, name_en, name_normalized, lat, lng
    FROM apartment_complexes
    ORDER BY LENGTH(name_normalized) DESC
  `);

  const complexes = complexRows.rows.map(r => ({
    id: r.id,
    nameRu: r.name_ru,
    nameUk: r.name_uk,
    nameEn: r.name_en,
    nameNormalized: r.name_normalized,
    patterns: buildSearchPatterns(r.name_ru, r.name_uk, r.name_en)
  }));

  console.log(`Loaded ${complexes.length} complexes\n`);

  // Count listings without complex_id
  const countResult = await client.query(`
    SELECT COUNT(*) FROM unified_listings WHERE complex_id IS NULL
  `);
  const totalWithoutComplex = parseInt(countResult.rows[0].count);
  console.log(`Listings without complex_id: ${totalWithoutComplex}\n`);

  // Process in batches
  const BATCH_SIZE = 5000;
  let processed = 0;
  let matched = 0;
  let offset = 0;

  console.log('Processing listings...\n');

  while (true) {
    const batch = await client.query(`
      SELECT id, description, lat, lng
      FROM unified_listings
      WHERE complex_id IS NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [BATCH_SIZE, offset]);

    if (batch.rows.length === 0) break;

    const updates = [];

    for (const row of batch.rows) {
      processed++;

      // Get text to search
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

      if (!searchText) continue;

      // Find complex by text
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

    // Progress
    const pct = ((processed / totalWithoutComplex) * 100).toFixed(1);
    process.stdout.write(`\rProcessed: ${processed}/${totalWithoutComplex} (${pct}%) | Matched: ${matched}`);
  }

  console.log('\n\nDone!');
  console.log(`Total processed: ${processed}`);
  console.log(`Total matched: ${matched} (${((matched/processed)*100).toFixed(1)}%)`);

  // Verify
  const verify = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(complex_id) as with_complex
    FROM unified_listings
  `);
  console.log(`\nAfter update: ${verify.rows[0].with_complex} of ${verify.rows[0].total} have complex_id`);

  await client.end();
}

main().catch(console.error);
