/**
 * GPT Street Recognition Test v4
 *
 * Изменения:
 * - GPT извлекает улицу
 * - Матчинг по полю names (содержит текущее + старые названия)
 * - Без второго GPT запроса на переименование
 */

const { Client } = require('pg');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');

// ====== CONFIG ======
const CONFIG = {
  testLimit: 1000,
  concurrency: 10,
  delayBetweenBatches: 1000,
  maxDescriptionLength: 1500,
  searchRadiusKm: 5,
  outputDir: path.join(__dirname, '../output/gpt-test-v4'),
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

ФОРМАТ ОТВЕТА (только JSON): {"street": "..."}

ПРИМЕРЫ:
"вул. Хрещатик 15" → {"street": "хрещатик"}
"Черёмушки, 5 этаж" → {"street": null}
"ул. Маршала Говорова 10" → {"street": "маршала говорова"}
"10 станція Фонтану" → {"street": null}`;

// ====== NORMALIZATION ======
function normalizeStreetName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
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

// ====== GPT API ======
async function extractStreetGPT(openai, description) {
  const startTime = Date.now();
  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: description.slice(0, CONFIG.maxDescriptionLength) }
      ],
      temperature: 0,
      max_tokens: 50,
      response_format: { type: 'json_object' }
    });

    const content = response.choices[0].message.content;
    const parsed = JSON.parse(content);

    return {
      success: true,
      street: parsed.street ? normalizeStreetName(parsed.street) : null,
      rawResponse: content,
      tokens: response.usage,
      latency: Date.now() - startTime
    };
  } catch (error) {
    return { success: false, error: error.message, latency: Date.now() - startTime };
  }
}

// ====== STREET MATCHING (по names.uk/ru - исправленные) ======
function findBestStreetMatch(extractedStreet, nearbyStreets) {
  if (!extractedStreet || !nearbyStreets || nearbyStreets.length === 0) return null;

  const normalized = normalizeStreetName(extractedStreet);
  const candidates = [];

  // Строим карту кандидатов из names.uk и names.ru (теперь исправлены)
  for (const s of nearbyStreets) {
    // UK названия - первое текущее, остальные старые
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

    // RU названия - первое текущее, остальные старые
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
  if (!process.env.OPENAI_API_KEY) {
    console.log('OPENAI_API_KEY не установлен!');
    return;
  }

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== GPT Street Recognition Test v4 ===');
  console.log('Матчинг по полю names (включает старые названия)\n');

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
  let matchedByOldName = 0;

  for (let i = 0; i < listings.length; i += CONFIG.concurrency) {
    const batch = listings.slice(i, i + CONFIG.concurrency);

    const batchPromises = batch.map(async (listing) => {
      const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

      if (!description) {
        return { id: listing.id, status: 'empty_description' };
      }

      // GPT извлекает улицу
      const extractResult = await extractStreetGPT(openai, description);
      totalTokens += extractResult.tokens?.total_tokens || 0;

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

      // Получаем улицы в радиусе с полями names.uk и names.ru (исправленные)
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

      // Матчинг по names
      const matchResult = findBestStreetMatch(extractResult.street, nearbyStreets);

      if (matchResult) {
        if (matchResult.isOldName) matchedByOldName++;

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

    // Progress
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
  stats.estimatedCost = (totalTokens / 1000 * 0.0005).toFixed(4);
  stats.processingTime = totalTime;

  // ====== SAVE ======
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  fs.writeFileSync(path.join(CONFIG.outputDir, `results-${timestamp}.json`), JSON.stringify(allResults, null, 2));
  fs.writeFileSync(path.join(CONFIG.outputDir, `summary-${timestamp}.json`), JSON.stringify(stats, null, 2));

  // ====== PRINT ======
  console.log('\n' + '='.repeat(60));
  console.log('РЕЗУЛЬТАТЫ ТЕСТА v4 (матчинг по names)');
  console.log('='.repeat(60));
  console.log(`Всего: ${stats.total} | Время: ${stats.processingTime}s`);
  console.log(`\n--- МАТЧИНГ ---`);
  console.log(`✅ Matched (current):   ${stats.matched}`);
  console.log(`✅ Matched (old name):  ${stats.matchedOldName} (${stats.oldNamePercent}%)`);
  console.log(`📊 ИТОГО matched:       ${stats.totalMatched} (${stats.totalMatchedPercent}%)`);
  console.log(`\n--- НЕ НАЙДЕНО ---`);
  console.log(`❌ No match in radius:  ${stats.noMatch}`);
  console.log(`❌ No street in text:   ${stats.noStreetFound}`);
  console.log(`\n--- СТОИМОСТЬ ---`);
  console.log(`Токенов: ${stats.totalTokens}`);
  console.log(`Стоимость: ~$${stats.estimatedCost}`);

  // Примеры matched by old name
  const oldNameExamples = allResults.filter(r => r.status === 'matched_old_name');
  if (oldNameExamples.length > 0) {
    console.log(`\n--- НАЙДЕНО ПО СТАРОМУ НАЗВАНИЮ ---`);
    oldNameExamples.slice(0, 10).forEach((r, i) => {
      console.log(`${i+1}. GPT: "${r.gptStreet}" → DB: "${r.matchedName}"`);
    });
  }

  await client.end();
  console.log('\n✅ Тест завершён');
}

main().catch(console.error);
