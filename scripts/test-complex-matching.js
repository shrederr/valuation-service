const { Client } = require('pg');

// Number mapping (for matching "48 перлина" to "сорок восьма перлина")
const NUMBER_WORDS = {
  '1': ['перша', 'первая', '1-а', '1-я'],
  '2': ['друга', 'вторая', '2-а', '2-я'],
  '3': ['третя', 'третья', '3-а', '3-я'],
  '4': ['четверта', 'четвёртая', '4-а', '4-я'],
  '5': ['п\'ята', 'пятая', '5-а', '5-я'],
  '6': ['шоста', 'шестая', '6-та', '6-я'],
  '7': ['сьома', 'сім', 'седьмая', 'семь', '7-а', '7-я'],
  '8': ['восьма', 'восьмая', '8-а', '8-я'],
  '9': ['дев\'ята', 'девятая', '9-а', '9-я'],
  '10': ['десята', 'десятая', '10-а', '10-я'],
  '11': ['одинадцята', 'оди|надцятая', '11-а'],
  '12': ['дванадцята', 'двенадцатая', '12-а'],
  '17': ['сімнадцята', '17-а'],
  '19': ['дев\'ятнадцята', '19-а'],
  '21': ['двадцять перша', '21-а'],
  '22': ['двадцять друга', '22-а'],
  '24': ['двадцять четверта', '24-а'],
  '25': ['двадцять п\'ята', '25-а'],
  '26': ['двадцять шоста', '26-та'],
  '27': ['двадцять сьома', '27-а'],
  '28': ['двадцять восьма', '28-а'],
  '29': ['двадцять дев\'ята', '29-а'],
  '31': ['тридцять перша', '31-я'],
  '32': ['тридцять друга', '32-а'],
  '34': ['тридцять четверта', '34-а'],
  '35': ['тридцять п\'ята', '35-а'],
  '36': ['тридцять шоста', '36-а'],
  '37': ['тридцять сьома', '37-а'],
  '40': ['сорокова', '40-а'],
  '41': ['сорок перша', '41-а'],
  '42': ['сорок друга', '42-га'],
  '43': ['сорок третя', '43-а'],
  '44': ['сорок четверта', '44-а'],
  '45': ['сорок п\'ята', '45-а'],
  '46': ['сорок шоста', '46-а'],
  '48': ['сорок восьма', '48-а'],
  '49': ['сорок дев\'ята', '49-а'],
  '50': ['п\'ятдесята', '50-а'],
  '51': ['п\'ятдесят перша', '51-а'],
};

/**
 * Clean name for matching (remove common prefixes)
 */
function cleanName(name) {
  return name
    .replace(/^(жк|жилой комплекс|житловий комплекс|кг|км|котеджне|коттеджное|містечко|городок|таунхаус[иі]?|дуплекс[иі]?)\s*/gi, '')
    .replace(/["«»'']/g, '')
    .replace(/\s*\([^)]+\)\s*/g, ' ')
    .replace(/\s*буд\.?\s*\d+/gi, '')
    .trim();
}

/**
 * Normalize numbers in text for matching
 */
function normalizeNumbers(text) {
  let result = text.toLowerCase();

  // Replace number words with digits
  for (const [num, words] of Object.entries(NUMBER_WORDS)) {
    for (const word of words) {
      const pattern = new RegExp(word.replace(/'/g, '[\'`]?'), 'gi');
      result = result.replace(pattern, num);
    }
  }

  return result;
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

    // Pattern 1: With ЖК prefix
    patterns.push(new RegExp(
      `(?:жк|жилой комплекс|житловий комплекс|кг|км)?\\s*["«']?${escapeRegex(cleaned)}["»']?`,
      'gi'
    ));

    // Pattern 2: Just the name
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
        const score = cleanedMatch.length / complex.normalizedLength;

        if (score > bestScore) {
          bestScore = score;
          bestMatch = {
            complexId: complex.id,
            complexName: complex.nameRu,
            matchedText,
            score: Math.min(score, 1),
          };
        }
      }
    }
  }

  return bestScore >= 0.5 ? bestMatch : null;
}

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });

  await client.connect();
  console.log('Connected to database\n');

  // Load complexes
  const rows = await client.query(`
    SELECT id, name_ru, name_uk, name_en, name_normalized, lat, lng
    FROM apartment_complexes
    ORDER BY LENGTH(name_normalized) DESC
  `);

  const complexes = rows.rows.map(r => ({
    id: r.id,
    nameRu: r.name_ru,
    nameUk: r.name_uk,
    nameEn: r.name_en,
    nameNormalized: r.name_normalized,
    normalizedLength: r.name_normalized.length,
    patterns: buildSearchPatterns(r.name_ru, r.name_uk, r.name_en),
  }));

  console.log(`Loaded ${complexes.length} complexes\n`);

  // Test cases
  const testCases = [
    'Продам 2-комнатную квартиру в ЖК 7 Самураев на Балковской',
    'Продам квартиру в ЖК "Сім самураїв" терміново',
    'Квартира в жилом комплексе Гагарин Плаза, отличный ремонт',
    'Продаю 1к в Аркадии, ЖК Costa fontana, вид на море',
    'ЖК Акварель-2, 3 комнаты, 85 кв.м',
    'Новострой на Таирова, ЖК Таирово',
    'Продам квартиру Кадор 32 перлина',
    'ЖК 48 перлина, 2 кімнати',
    'Квартира в Unity Towers с панорамным видом',
    'Продам в Artville, 2 комнаты',
    'Без посередників, в ЖК Smart House',
    'Квартира в новобудові Дім біля моря Premier',
  ];

  console.log('=== Testing Complex Matching ===\n');

  for (const text of testCases) {
    const result = findComplexInText(text, complexes);
    if (result) {
      console.log(`✓ "${text.substring(0, 60)}..."`);
      console.log(`  → ${result.complexName} (score: ${result.score.toFixed(2)}, matched: "${result.matchedText}")\n`);
    } else {
      console.log(`✗ "${text.substring(0, 60)}..." - NO MATCH\n`);
    }
  }

  // Test with real OLX data
  console.log('\n=== Testing with Real OLX Titles ===\n');

  const olxTitles = await client.query(`
    SELECT title, description, external_url
    FROM aggregator_import
    WHERE external_url LIKE '%olx.ua%'
      AND is_active = 't'
      AND (title ILIKE '%жк%' OR title ILIKE '%житловий%' OR description ILIKE '%жк%')
    LIMIT 20
  `);

  let matched = 0;
  for (const row of olxTitles.rows) {
    const text = `${row.title || ''} ${(row.description || '').substring(0, 500)}`;
    const result = findComplexInText(text, complexes);

    if (result) {
      matched++;
      console.log(`✓ OLX: "${row.title?.substring(0, 50)}..."`);
      console.log(`  → ${result.complexName} (score: ${result.score.toFixed(2)})\n`);
    }
  }

  console.log(`\nMatched ${matched} of ${olxTitles.rows.length} OLX listings with ЖК mentions`);

  await client.end();
}

main().catch(console.error);
