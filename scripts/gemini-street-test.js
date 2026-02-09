/**
 * Gemini Street Recognition Test
 *
 * Использует Gemini вместо GPT для извлечения улиц
 */

const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

// ====== CONFIG ======
const CONFIG = {
  testLimit: 20, // Тест
  concurrency: 1, // По одному запросу
  delayBetweenBatches: 15000, // 15 секунд между запросами (медленный режим)
  maxDescriptionLength: 1500,
  searchRadiusKm: 5,
  outputDir: path.join(__dirname, '../output/gemini-test'),
  odessa: {
    latMin: 46.3,
    latMax: 46.6,
    lngMin: 30.6,
    lngMax: 30.9
  }
};

const SYSTEM_PROMPT = `Ты помощник для извлечения адресов из украинских объявлений о недвижимости.

ЗАДАЧА: Извлеки улицу из текста.

ПРАВИЛА:
1. Улицу в именительном падеже, без префиксов (вул., ул., просп., пр-т, б-р, пров., пер.)
2. Если не указана явно - ставь null
3. ЖК - это НЕ улица
4. Районы (Черёмушки, Таирова, Аркадия, Поскот) - это НЕ улицы
5. Станции Фонтана, номера микрорайонов - это НЕ улицы
6. Если указаны две улицы через "/" - бери первую
7. ВАЖНО: Раскрывай сокращения! м. → маршала, ген. → генерала, акад. → академіка, адм. → адмірала

ФОРМАТ ОТВЕТА (только JSON): {"street": "..."}

ПРИМЕРЫ:
"вул. Хрещатик 15" → {"street": "хрещатик"}
"Черёмушки, 5 этаж" → {"street": null}
"ул. Маршала Говорова 10" → {"street": "маршала говорова"}
"ул. М.Говорова 5" → {"street": "маршала говорова"}
"ген. Петрова 3" → {"street": "генерала петрова"}
"10 станція Фонтану" → {"street": null}`;

// ====== NORMALIZATION ======
// Abbreviation expansions for street name matching
const ABBREVIATIONS = {
  'м.': 'маршала',
  'м ': 'маршала ',
  'ген.': 'генерала',
  'ген ': 'генерала ',
  'акад.': 'академіка',
  'акад ': 'академіка ',
  'ак.': 'академіка',
  'ак ': 'академіка ',
  'проф.': 'професора',
  'проф ': 'професора ',
  'кн.': 'князя',
  'адм.': 'адмірала',
  'гетьм.': 'гетьмана',
  'полк.': 'полковника',
  'капит.': 'капітана',
};

function expandAbbreviations(name) {
  let result = name.toLowerCase();
  for (const [abbr, full] of Object.entries(ABBREVIATIONS)) {
    result = result.replace(new RegExp(abbr.replace('.', '\\.'), 'gi'), full);
  }
  return result;
}

function normalizeStreetName(name) {
  if (!name) return '';
  let normalized = name.toLowerCase();

  // Expand abbreviations first
  normalized = expandAbbreviations(normalized);

  return normalized
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
    .replace(/\s+(вулиця|улица|проспект|провулок|переулок|бульвар|площа|площадь|набережна|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)$/gi, '')
    .replace(/[«»""''`']/g, '')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

function levenshtein(a, b) {
  if (!a || !b) return Infinity;
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      matrix[i][j] = b[i-1] === a[j-1]
        ? matrix[i-1][j-1]
        : Math.min(matrix[i-1][j-1] + 1, matrix[i][j-1] + 1, matrix[i-1][j] + 1);
    }
  }
  return matrix[b.length][a.length];
}

// ====== GEMINI API ======
async function extractStreetGemini(apiKey, description) {
  const startTime = Date.now();

  try {
    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{
            parts: [{
              text: `${SYSTEM_PROMPT}\n\nТекст объявления:\n${description.slice(0, CONFIG.maxDescriptionLength)}`
            }]
          }],
          generationConfig: {
            temperature: 0,
            maxOutputTokens: 1000,
          }
        })
      }
    );

    const data = await response.json();

    if (data.error) {
      return { success: false, error: data.error.message, latency: Date.now() - startTime };
    }

    // Gemini 2.5 может иметь несколько parts (thinking + response)
    const parts = data.candidates?.[0]?.content?.parts || [];
    let content = '';
    for (const part of parts) {
      if (part.text) content += part.text + '\n';
    }
    content = content.trim();

    if (!content) {
      return { success: false, error: 'Empty response', rawResponse: JSON.stringify(data), latency: Date.now() - startTime };
    }

    // Парсим JSON из ответа
    const jsonMatch = content.match(/\{[^}]+\}/);
    if (!jsonMatch) {
      // Попробуем найти улицу напрямую в тексте
      const streetMatch = content.match(/street["\s:]+["']?([^"'\n}]+)/i);
      if (streetMatch) {
        const street = streetMatch[1].trim();
        if (street && street !== 'null') {
          return {
            success: true,
            street: normalizeStreetName(street),
            rawResponse: content,
            tokens: data.usageMetadata?.totalTokenCount || 0,
            latency: Date.now() - startTime
          };
        }
      }
      return { success: false, error: 'No JSON in response', rawResponse: content.slice(0, 200), latency: Date.now() - startTime };
    }

    const parsed = JSON.parse(jsonMatch[0]);

    return {
      success: true,
      street: parsed.street ? normalizeStreetName(parsed.street) : null,
      rawResponse: content,
      tokens: data.usageMetadata?.totalTokenCount || 0,
      latency: Date.now() - startTime
    };
  } catch (error) {
    return { success: false, error: error.message, latency: Date.now() - startTime };
  }
}

// ====== STREET MATCHING ======
function findBestStreetMatch(extractedStreet, nearbyStreets) {
  if (!extractedStreet || !nearbyStreets || nearbyStreets.length === 0) return null;

  const normalized = normalizeStreetName(extractedStreet);
  const candidates = [];

  for (const s of nearbyStreets) {
    const ukNames = s.names_uk || [];
    ukNames.forEach((name, idx) => {
      const norm = normalizeStreetName(name);
      if (norm && norm.length >= 3) {
        candidates.push({
          street: s,
          normalized: norm,
          original: name,
          isOldName: idx > 0,
          lang: 'uk'
        });
      }
    });

    const ruNames = s.names_ru || [];
    ruNames.forEach((name, idx) => {
      const norm = normalizeStreetName(name);
      if (norm && norm.length >= 3) {
        candidates.push({
          street: s,
          normalized: norm,
          original: name,
          isOldName: idx > 0,
          lang: 'ru'
        });
      }
    });
  }

  // 1. Exact match
  for (const c of candidates) {
    if (c.normalized === normalized) {
      return {
        match: c.street,
        type: 'exact',
        matchedName: c.original,
        isOldName: c.isOldName,
        similarity: 1.0,
        distance: c.street.distance_km
      };
    }
  }

  // 2. Fuzzy match (Levenshtein ≤ 2)
  let bestMatch = null;
  let bestDistance = Infinity;

  for (const c of candidates) {
    if (Math.abs(c.normalized.length - normalized.length) > 3) continue;
    const dist = levenshtein(normalized, c.normalized);
    if (dist < bestDistance && dist <= 2) {
      bestDistance = dist;
      bestMatch = c;
    }
  }

  if (bestMatch) {
    return {
      match: bestMatch.street,
      type: 'fuzzy',
      matchedName: bestMatch.original,
      isOldName: bestMatch.isOldName,
      similarity: 1 - (bestDistance / Math.max(normalized.length, bestMatch.normalized.length)),
      distance: bestMatch.street.distance_km
    };
  }

  // 3. Substring match
  for (const c of candidates) {
    if (c.normalized.length >= 4 && (normalized.includes(c.normalized) || c.normalized.includes(normalized))) {
      return {
        match: c.street,
        type: 'substring',
        matchedName: c.original,
        isOldName: c.isOldName,
        similarity: 0.7,
        distance: c.street.distance_km
      };
    }
  }

  return null;
}

// ====== MAIN ======
async function main() {
  const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_AI_KEY;

  if (!apiKey) {
    console.log('GEMINI_API_KEY не установлен!');
    console.log('Запуск: GEMINI_API_KEY=... node scripts/gemini-street-test.js');
    return;
  }

  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Gemini Street Recognition Test ===');
  console.log('Model: gemini-1.5-flash\n');

  if (!fs.existsSync(CONFIG.outputDir)) {
    fs.mkdirSync(CONFIG.outputDir, { recursive: true });
  }

  // ====== GET ODESSA LISTINGS ======
  console.log(`Fetching ${CONFIG.testLimit} OLX listings in Odessa without street_id...`);
  const listingsResult = await client.query(`
    SELECT id, lat, lng,
           description->>'uk' as desc_uk,
           description->>'ru' as desc_ru,
           geo_id
    FROM unified_listings
    WHERE realty_platform = 'olx'
      AND street_id IS NULL
      AND lat BETWEEN $1 AND $2
      AND lng BETWEEN $3 AND $4
      AND lat IS NOT NULL AND lng IS NOT NULL
    ORDER BY id
    LIMIT $5
  `, [CONFIG.odessa.latMin, CONFIG.odessa.latMax, CONFIG.odessa.lngMin, CONFIG.odessa.lngMax, CONFIG.testLimit]);

  const listings = listingsResult.rows;
  console.log(`Got ${listings.length} listings\n`);

  // ====== PROCESS ======
  const startTime = Date.now();
  const allResults = [];
  let totalTokens = 0;

  for (let i = 0; i < listings.length; i += CONFIG.concurrency) {
    const batch = listings.slice(i, i + CONFIG.concurrency);

    const batchPromises = batch.map(async (listing) => {
      const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

      if (!description) {
        return { id: listing.id, status: 'empty_description' };
      }

      const extractResult = await extractStreetGemini(apiKey, description);
      totalTokens += extractResult.tokens || 0;

      if (!extractResult.success) {
        return { id: listing.id, status: 'api_error', error: extractResult.error };
      }

      if (!extractResult.street) {
        return {
          id: listing.id,
          status: 'no_street_found',
          description: description.slice(0, 200)
        };
      }

      const nearbyStreetsResult = await client.query(`
        SELECT s.id, s.geo_id,
               s.names->'uk' as names_uk,
               s.names->'ru' as names_ru,
               ST_Distance(ST_Centroid(s.line)::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) / 1000 as distance_km
        FROM streets s
        WHERE ST_DWithin(ST_Centroid(s.line)::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
        ORDER BY distance_km
      `, [listing.lng, listing.lat, CONFIG.searchRadiusKm * 1000]);

      const nearbyStreets = nearbyStreetsResult.rows;
      const matchResult = findBestStreetMatch(extractResult.street, nearbyStreets);

      if (matchResult) {
        return {
          id: listing.id,
          status: matchResult.isOldName ? 'matched_old_name' : 'matched',
          gptStreet: extractResult.street,
          matchedStreetId: matchResult.match.id,
          matchedName: matchResult.matchedName,
          matchType: matchResult.type,
          isOldName: matchResult.isOldName,
          distanceKm: matchResult.distance?.toFixed(2)
        };
      }

      return {
        id: listing.id,
        status: 'no_match',
        gptStreet: extractResult.street,
        nearbyCount: nearbyStreets.length,
        description: description.slice(0, 200)
      };
    });

    const batchResults = await Promise.all(batchPromises);
    allResults.push(...batchResults);

    const progress = Math.min(i + CONFIG.concurrency, listings.length);
    const matched = allResults.filter(r => r.status === 'matched' || r.status === 'matched_old_name').length;
    const oldName = allResults.filter(r => r.status === 'matched_old_name').length;
    console.log(`Progress: ${progress}/${listings.length} | Matched: ${matched} (old: ${oldName})`);

    if (i + CONFIG.concurrency < listings.length) {
      await new Promise(r => setTimeout(r, CONFIG.delayBetweenBatches));
    }
  }

  // ====== STATS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  const stats = {
    total: allResults.length,
    matched: allResults.filter(r => r.status === 'matched').length,
    matchedOldName: allResults.filter(r => r.status === 'matched_old_name').length,
    noMatch: allResults.filter(r => r.status === 'no_match').length,
    noStreetFound: allResults.filter(r => r.status === 'no_street_found').length,
    emptyDescription: allResults.filter(r => r.status === 'empty_description').length,
    apiError: allResults.filter(r => r.status === 'api_error').length,
  };

  stats.totalMatched = stats.matched + stats.matchedOldName;
  stats.totalMatchedPercent = ((stats.totalMatched / stats.total) * 100).toFixed(1);
  stats.oldNamePercent = ((stats.matchedOldName / stats.total) * 100).toFixed(1);
  stats.totalTokens = totalTokens;
  stats.processingTime = totalTime;

  // ====== SAVE ======
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  fs.writeFileSync(path.join(CONFIG.outputDir, `results-${timestamp}.json`), JSON.stringify(allResults, null, 2));
  fs.writeFileSync(path.join(CONFIG.outputDir, `summary-${timestamp}.json`), JSON.stringify(stats, null, 2));

  // ====== PRINT ======
  console.log('\n' + '='.repeat(60));
  console.log('РЕЗУЛЬТАТЫ ТЕСТА Gemini');
  console.log('='.repeat(60));
  console.log(`Всего: ${stats.total} | Время: ${stats.processingTime}s`);
  console.log(`\n--- МАТЧИНГ ---`);
  console.log(`✅ Matched (current):   ${stats.matched}`);
  console.log(`✅ Matched (old name):  ${stats.matchedOldName} (${stats.oldNamePercent}%)`);
  console.log(`📊 ИТОГО matched:       ${stats.totalMatched} (${stats.totalMatchedPercent}%)`);
  console.log(`\n--- НЕ НАЙДЕНО ---`);
  console.log(`❌ No match in radius:  ${stats.noMatch}`);
  console.log(`❌ No street in text:   ${stats.noStreetFound}`);
  console.log(`❌ API errors:          ${stats.apiError}`);
  console.log(`\n--- ТОКЕНЫ ---`);
  console.log(`Всего токенов: ${stats.totalTokens}`);

  const oldNameExamples = allResults.filter(r => r.status === 'matched_old_name');
  if (oldNameExamples.length > 0) {
    console.log(`\n--- НАЙДЕНО ПО СТАРОМУ НАЗВАНИЮ ---`);
    oldNameExamples.slice(0, 10).forEach((r, i) => {
      console.log(`${i+1}. Gemini: "${r.gptStreet}" → DB: "${r.matchedName}"`);
    });
  }

  await client.end();
  console.log('\n✅ Тест завершён');
}

main().catch(console.error);
