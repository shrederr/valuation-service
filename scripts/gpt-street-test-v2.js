/**
 * GPT Street Recognition Test v2
 *
 * Улучшения:
 * 1. Только объекты из Одессы (по координатам)
 * 2. GPT извлекает населённый пункт + улицу
 * 3. Поиск улицы в радиусе 5км от координат объекта
 * 4. Лимит 1000 объектов
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
  searchRadiusKm: 5, // радиус поиска улиц от координат объекта
  outputDir: path.join(__dirname, '../output/gpt-test-v2'),
  // Одесса по координатам
  odessa: {
    latMin: 46.3,
    latMax: 46.6,
    lngMin: 30.6,
    lngMax: 30.9
  }
};

const SYSTEM_PROMPT = `Ты помощник для извлечения адресов из украинских объявлений о недвижимости.

ЗАДАЧА: Из текста описания извлеки:
1. Населённый пункт (город, село, посёлок) - если указан
2. Улицу/проспект/бульвар (без номера дома)

ПРАВИЛА:
1. Название улицы в именительном падеже, без префиксов (вул., ул., просп., пр-т, бульв., пров., пер.)
2. Если населённый пункт или улица не указаны явно - ставь null
3. Не путай ЖК с улицами
4. Район/микрорайон - это НЕ улица (Черёмушки, Таирова, Аркадия - это районы)
5. Станции Фонтана - это районы, не улицы

ФОРМАТ ОТВЕТА (только JSON):
{"settlement": "название населённого пункта или null", "street": "название улицы или null"}

ПРИМЕРЫ:
"Продам квартиру на вул. Хрещатик 15, Київ" → {"settlement": "київ", "street": "хрещатик"}
"2к квартира, проспект Перемоги, 25" → {"settlement": null, "street": "перемоги"}
"Оренда в Одесі біля метро" → {"settlement": "одеса", "street": null}
"ЖК Комфорт Таун, новобудова" → {"settlement": null, "street": null}
"Черёмушки, 5 этаж" → {"settlement": null, "street": null}
"пр-т Шевченка 30, м. Львів" → {"settlement": "львів", "street": "шевченка"}
"с. Фонтанка, Одеська область" → {"settlement": "фонтанка", "street": null}
"10 станція Великого Фонтану" → {"settlement": null, "street": null}
"вул. Французький бульвар 22" → {"settlement": null, "street": "французький бульвар"}`;

// ====== NORMALIZATION ======
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

function normalizeSettlement(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/^(місто|м\.|город|г\.|село|с\.|селище|смт|посёлок|пос\.|пгт)\s*/gi, '')
    .replace(/[«»""''`']/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Levenshtein distance для fuzzy matching
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
async function extractAddressGPT(openai, description) {
  const startTime = Date.now();

  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: description.slice(0, CONFIG.maxDescriptionLength) }
      ],
      temperature: 0,
      max_tokens: 100,
      response_format: { type: 'json_object' }
    });

    const content = response.choices[0].message.content;
    const parsed = JSON.parse(content);
    const latency = Date.now() - startTime;

    return {
      success: true,
      settlement: parsed.settlement ? normalizeSettlement(parsed.settlement) : null,
      street: parsed.street ? normalizeStreetName(parsed.street) : null,
      rawResponse: content,
      tokens: response.usage,
      latency
    };
  } catch (error) {
    return {
      success: false,
      error: error.message,
      latency: Date.now() - startTime
    };
  }
}

// ====== STREET MATCHING (в радиусе от координат) ======
function findBestStreetMatch(extractedStreet, nearbyStreets) {
  if (!extractedStreet || !nearbyStreets || nearbyStreets.length === 0) return null;

  const normalized = normalizeStreetName(extractedStreet);

  // Build map from nearby streets
  const streetNameMap = new Map();
  for (const s of nearbyStreets) {
    const normUk = normalizeStreetName(s.name_uk);
    const normRu = normalizeStreetName(s.name_ru);
    if (normUk && normUk.length >= 3) streetNameMap.set(normUk, s);
    if (normRu && normRu.length >= 3) streetNameMap.set(normRu, s);
  }

  // 1. Exact match
  if (streetNameMap.has(normalized)) {
    const s = streetNameMap.get(normalized);
    return { match: s, type: 'exact', similarity: 1.0, distance: s.distance_km };
  }

  // 2. Fuzzy match (Levenshtein distance ≤ 2)
  let bestMatch = null;
  let bestDistance = Infinity;
  let bestKey = null;

  for (const [key, street] of streetNameMap.entries()) {
    if (Math.abs(key.length - normalized.length) > 3) continue;
    const distance = levenshtein(normalized, key);
    if (distance < bestDistance && distance <= 2) {
      bestDistance = distance;
      bestMatch = street;
      bestKey = key;
    }
  }

  if (bestMatch) {
    return {
      match: bestMatch,
      type: 'fuzzy',
      similarity: 1 - (bestDistance / Math.max(normalized.length, bestKey.length)),
      matchedAs: bestKey,
      distance: bestMatch.distance_km
    };
  }

  // 3. Substring match
  for (const [key, street] of streetNameMap.entries()) {
    if (key.length >= 4 && (normalized.includes(key) || key.includes(normalized))) {
      return { match: street, type: 'substring', similarity: 0.7, matchedAs: key, distance: street.distance_km };
    }
  }

  return null;
}

// ====== MAIN ======
async function main() {
  // Check for API key
  if (!process.env.OPENAI_API_KEY) {
    console.log('=== GPT Street Recognition Test v2 ===\n');
    console.log('⚠️  OPENAI_API_KEY не установлен!\n');
    console.log('Запуск: OPENAI_API_KEY=sk-... node scripts/gpt-street-test-v2.js\n');
    return;
  }

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== GPT Street Recognition Test v2 ===');
  console.log('Логика: GPT → населённый пункт + улица → поиск в радиусе 5км\n');

  // Create output directory
  if (!fs.existsSync(CONFIG.outputDir)) {
    fs.mkdirSync(CONFIG.outputDir, { recursive: true });
  }

  // ====== GET ODESSA LISTINGS WITHOUT STREET_ID ======
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
      AND lat IS NOT NULL
      AND lng IS NOT NULL
    ORDER BY id
    LIMIT $5
  `, [CONFIG.odessa.latMin, CONFIG.odessa.latMax, CONFIG.odessa.lngMin, CONFIG.odessa.lngMax, CONFIG.testLimit]);

  const listings = listingsResult.rows;
  console.log(`Got ${listings.length} Odessa listings for testing\n`);

  if (listings.length === 0) {
    console.log('No listings found!');
    await client.end();
    return;
  }

  // ====== PROCESS ======
  console.log('Starting GPT extraction with location-based street search...');
  const startTime = Date.now();
  const allResults = [];

  const batchSize = CONFIG.concurrency;
  for (let i = 0; i < listings.length; i += batchSize) {
    const batch = listings.slice(i, i + batchSize);

    const batchPromises = batch.map(listing =>
      (async () => {
        const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

        if (!description) {
          return {
            id: listing.id,
            lat: listing.lat,
            lng: listing.lng,
            geo_id: listing.geo_id,
            status: 'empty_description',
            description: '',
            gptResult: null,
            matchResult: null,
            nearbyStreetsCount: 0
          };
        }

        // Call GPT
        const gptResult = await extractAddressGPT(openai, description);

        let matchResult = null;
        let status = 'unknown';
        let nearbyStreetsCount = 0;

        if (!gptResult.success) {
          status = 'api_error';
        } else if (!gptResult.street) {
          status = 'no_street_found';
        } else {
          // Find streets within radius of listing coordinates
          const nearbyStreetsResult = await client.query(`
            SELECT
              s.id, s.geo_id,
              s.name->>'uk' as name_uk,
              s.name->>'ru' as name_ru,
              ST_Distance(
                ST_Centroid(s.line)::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
              ) / 1000 as distance_km
            FROM streets s
            WHERE ST_DWithin(
              ST_Centroid(s.line)::geography,
              ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
              $3
            )
            ORDER BY distance_km
          `, [listing.lng, listing.lat, CONFIG.searchRadiusKm * 1000]);

          nearbyStreetsCount = nearbyStreetsResult.rows.length;

          // Try to match
          matchResult = findBestStreetMatch(gptResult.street, nearbyStreetsResult.rows);

          if (matchResult) {
            status = 'matched';
          } else if (nearbyStreetsCount === 0) {
            status = 'no_streets_nearby';
          } else {
            status = 'no_match_in_radius';
          }
        }

        return {
          id: listing.id,
          lat: listing.lat,
          lng: listing.lng,
          geo_id: listing.geo_id,
          status,
          description: description.slice(0, 500),
          gptResult: {
            settlement: gptResult.settlement,
            extractedStreet: gptResult.street,
            rawResponse: gptResult.rawResponse,
            tokens: gptResult.tokens,
            latency: gptResult.latency,
            error: gptResult.error
          },
          matchResult: matchResult ? {
            streetId: matchResult.match.id,
            geoId: matchResult.match.geo_id,
            matchType: matchResult.type,
            similarity: matchResult.similarity,
            matchedAs: matchResult.matchedAs,
            dbStreetName: matchResult.match.name_uk || matchResult.match.name_ru,
            distanceKm: matchResult.distance
          } : null,
          nearbyStreetsCount
        };
      })()
    );

    const batchResults = await Promise.all(batchPromises);
    allResults.push(...batchResults);

    // Progress
    const progress = Math.min(i + batchSize, listings.length);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const matched = allResults.filter(r => r.status === 'matched').length;
    const noMatch = allResults.filter(r => r.status === 'no_match_in_radius').length;
    const noStreet = allResults.filter(r => r.status === 'no_street_found').length;

    console.log(`Progress: ${progress}/${listings.length} | Matched: ${matched} | NoMatchRadius: ${noMatch} | NoStreet: ${noStreet} | ${elapsed}s`);

    if (i + batchSize < listings.length) {
      await new Promise(r => setTimeout(r, CONFIG.delayBetweenBatches));
    }
  }

  // ====== CALCULATE STATS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  const stats = {
    total: allResults.length,
    matched: allResults.filter(r => r.status === 'matched').length,
    noMatchInRadius: allResults.filter(r => r.status === 'no_match_in_radius').length,
    noStreetsNearby: allResults.filter(r => r.status === 'no_streets_nearby').length,
    noStreetFound: allResults.filter(r => r.status === 'no_street_found').length,
    emptyDescription: allResults.filter(r => r.status === 'empty_description').length,
    apiError: allResults.filter(r => r.status === 'api_error').length,
  };

  stats.matchedPercent = ((stats.matched / stats.total) * 100).toFixed(1);
  stats.noMatchPercent = ((stats.noMatchInRadius / stats.total) * 100).toFixed(1);
  stats.noStreetPercent = ((stats.noStreetFound / stats.total) * 100).toFixed(1);
  stats.gptFoundStreet = stats.matched + stats.noMatchInRadius + stats.noStreetsNearby;
  stats.gptFoundStreetPercent = ((stats.gptFoundStreet / stats.total) * 100).toFixed(1);

  // Token stats
  const successResults = allResults.filter(r => r.gptResult?.tokens);
  const totalTokens = successResults.reduce((sum, r) => sum + (r.gptResult?.tokens?.total_tokens || 0), 0);
  const avgLatency = successResults.reduce((sum, r) => sum + (r.gptResult?.latency || 0), 0) / successResults.length;

  stats.totalTokens = totalTokens;
  stats.avgTokensPerRequest = Math.round(totalTokens / successResults.length);
  stats.avgLatencyMs = Math.round(avgLatency);
  stats.estimatedCost = (totalTokens / 1000 * 0.0005).toFixed(4);
  stats.processingTime = totalTime;
  stats.searchRadiusKm = CONFIG.searchRadiusKm;

  // Match type breakdown
  const matchedResults = allResults.filter(r => r.status === 'matched');
  stats.matchTypes = {
    exact: matchedResults.filter(r => r.matchResult?.matchType === 'exact').length,
    fuzzy: matchedResults.filter(r => r.matchResult?.matchType === 'fuzzy').length,
    substring: matchedResults.filter(r => r.matchResult?.matchType === 'substring').length
  };

  // Settlement stats
  const withSettlement = allResults.filter(r => r.gptResult?.settlement);
  stats.withSettlement = withSettlement.length;

  // ====== SAVE RESULTS ======
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-results-${timestamp}.json`),
    JSON.stringify(allResults, null, 2)
  );

  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-summary-${timestamp}.json`),
    JSON.stringify(stats, null, 2)
  );

  // No match in radius (for analysis)
  const noMatchResults = allResults.filter(r => r.status === 'no_match_in_radius');
  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-no-match-${timestamp}.json`),
    JSON.stringify(noMatchResults, null, 2)
  );

  // ====== PRINT SUMMARY ======
  console.log('\n' + '='.repeat(60));
  console.log('РЕЗУЛЬТАТЫ ТЕСТА v2 (Одесса, радиус 5км)');
  console.log('='.repeat(60));
  console.log(`\nВсего обработано: ${stats.total}`);
  console.log(`Время обработки: ${stats.processingTime}s`);
  console.log(`Радиус поиска улиц: ${stats.searchRadiusKm}км`);
  console.log(`\n--- СТАТУСЫ ---`);
  console.log(`✅ Matched (найдено в радиусе):  ${stats.matched} (${stats.matchedPercent}%)`);
  console.log(`⚠️  No Match in Radius:          ${stats.noMatchInRadius} (${stats.noMatchPercent}%)`);
  console.log(`🔍 No Streets Nearby:            ${stats.noStreetsNearby}`);
  console.log(`❌ No Street Found (GPT null):   ${stats.noStreetFound} (${stats.noStreetPercent}%)`);
  console.log(`📭 Empty Description:            ${stats.emptyDescription}`);
  console.log(`🔴 API Errors:                   ${stats.apiError}`);
  console.log(`\n--- ЭФФЕКТИВНОСТЬ ---`);
  console.log(`GPT нашёл улицу: ${stats.gptFoundStreetPercent}% (${stats.gptFoundStreet} из ${stats.total})`);
  console.log(`GPT нашёл населённый пункт: ${stats.withSettlement} записей`);
  console.log(`Успешный матчинг в радиусе: ${stats.matchedPercent}%`);
  console.log(`\n--- ТИПЫ МАТЧИНГА ---`);
  console.log(`Exact match:     ${stats.matchTypes.exact}`);
  console.log(`Fuzzy match:     ${stats.matchTypes.fuzzy}`);
  console.log(`Substring match: ${stats.matchTypes.substring}`);
  console.log(`\n--- СТОИМОСТЬ ---`);
  console.log(`Всего токенов: ${stats.totalTokens}`);
  console.log(`Среднее на запрос: ${stats.avgTokensPerRequest} токенов`);
  console.log(`Средняя задержка: ${stats.avgLatencyMs}ms`);
  console.log(`Стоимость теста: ~$${stats.estimatedCost}`);

  console.log(`\n--- ФАЙЛЫ ---`);
  console.log(`Все результаты: ${CONFIG.outputDir}/gpt-test-results-${timestamp}.json`);
  console.log(`Статистика: ${CONFIG.outputDir}/gpt-test-summary-${timestamp}.json`);
  console.log(`Не найдено в радиусе: ${CONFIG.outputDir}/gpt-test-no-match-${timestamp}.json`);

  // Print examples of no-match
  if (noMatchResults.length > 0) {
    console.log(`\n--- ПРИМЕРЫ "NO MATCH IN RADIUS" (первые 10) ---`);
    noMatchResults.slice(0, 10).forEach((r, i) => {
      console.log(`${i+1}. GPT: settlement="${r.gptResult?.settlement}", street="${r.gptResult?.extractedStreet}"`);
      console.log(`   Координаты: ${r.lat}, ${r.lng} | Улиц в радиусе: ${r.nearbyStreetsCount}`);
      console.log(`   Описание: ${r.description?.slice(0, 80)}...`);
    });
  }

  await client.end();
  console.log('\n✅ Тест завершён');
}

main().catch(console.error);
