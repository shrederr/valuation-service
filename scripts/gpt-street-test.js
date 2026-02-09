/**
 * GPT Street Recognition Test
 *
 * Тестовый прогон на 1000 OLX объектов без street_id
 * Цель: проверить эффективность GPT для распознавания улиц
 *
 * Результаты сохраняются в:
 * - gpt-test-results.json (все результаты)
 * - gpt-test-summary.json (статистика)
 * - gpt-test-no-match.json (GPT нашёл улицу, но нет матчинга в БД)
 */

const { Client } = require('pg');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');

// ====== CONFIG ======
const CONFIG = {
  testLimit: 1000,
  concurrency: 10, // параллельных запросов
  delayBetweenBatches: 1000, // ms между батчами
  maxDescriptionLength: 1500, // обрезаем длинные описания
  outputDir: path.join(__dirname, '../output/gpt-test')
};

const SYSTEM_PROMPT = `Ты помощник для извлечения адресов из украинских объявлений о недвижимости.

ЗАДАЧА: Из текста описания извлеки ТОЛЬКО название улицы/проспекта/бульвара (без номера дома, без города).

ПРАВИЛА:
1. Извлекай улицу в именительном падеже
2. Убирай префиксы (вул., ул., просп., пр-т, бульв., пров., пер.)
3. Если улица не упоминается явно - верни null
4. Не путай названия ЖК с улицами
5. Не угадывай улицу по району или метро

ФОРМАТ ОТВЕТА (только JSON):
{"street": "название улицы"} или {"street": null}

ПРИМЕРЫ:
"Продам квартиру на вул. Хрещатик 15" → {"street": "хрещатик"}
"2к квартира, проспект Перемоги, 25" → {"street": "перемоги"}
"Оренда біля метро Оболонь" → {"street": null}
"ЖК Комфорт Таун, новобудова" → {"street": null}
"пр-т Науки 30, поруч з метро" → {"street": "науки"}
"Подол, исторический центр" → {"street": null}`;

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
    const latency = Date.now() - startTime;

    return {
      success: true,
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

// ====== STREET MATCHING ======
function findBestStreetMatch(extractedStreet, streetNameMap) {
  if (!extractedStreet) return null;

  const normalized = normalizeStreetName(extractedStreet);

  // 1. Exact match
  if (streetNameMap.has(normalized)) {
    return { match: streetNameMap.get(normalized), type: 'exact', similarity: 1.0 };
  }

  // 2. Fuzzy match (Levenshtein distance ≤ 2)
  let bestMatch = null;
  let bestDistance = Infinity;
  let bestKey = null;

  for (const [key, street] of streetNameMap.entries()) {
    // Skip very different lengths
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
      matchedAs: bestKey
    };
  }

  // 3. Substring match (street name contains or is contained)
  for (const [key, street] of streetNameMap.entries()) {
    if (key.length >= 4 && (normalized.includes(key) || key.includes(normalized))) {
      return { match: street, type: 'substring', similarity: 0.7, matchedAs: key };
    }
  }

  return null;
}

// ====== BATCH PROCESSING ======
async function processBatch(openai, listings, streetNameMap) {
  const results = [];

  for (const listing of listings) {
    const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

    if (!description) {
      results.push({
        id: listing.id,
        status: 'empty_description',
        gptResult: null,
        matchResult: null
      });
      continue;
    }

    // Call GPT
    const gptResult = await extractStreetGPT(openai, description);

    let matchResult = null;
    let status = 'unknown';

    if (!gptResult.success) {
      status = 'api_error';
    } else if (!gptResult.street) {
      status = 'no_street_found';
    } else {
      // Try to match with our DB
      matchResult = findBestStreetMatch(gptResult.street, streetNameMap);

      if (matchResult) {
        status = 'matched';
      } else {
        status = 'no_match_in_db';
      }
    }

    results.push({
      id: listing.id,
      status,
      description: description.slice(0, 500), // сохраняем часть для анализа
      gptResult: {
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
        dbStreetName: matchResult.match.nameUk || matchResult.match.nameRu
      } : null
    });
  }

  return results;
}

// ====== MAIN ======
async function main() {
  // Check for API key
  if (!process.env.OPENAI_API_KEY) {
    console.log('=== GPT Street Recognition Test ===\n');
    console.log('⚠️  OPENAI_API_KEY не установлен!\n');
    console.log('Для запуска теста выполните:');
    console.log('  Windows CMD:   set OPENAI_API_KEY=sk-... && node scripts/gpt-street-test.js');
    console.log('  Windows PS:    $env:OPENAI_API_KEY="sk-..."; node scripts/gpt-street-test.js');
    console.log('  Git Bash:      OPENAI_API_KEY=sk-... node scripts/gpt-street-test.js\n');
    console.log('Скрипт подготовлен и готов к запуску.');
    return;
  }

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== GPT Street Recognition Test ===\n');

  // Create output directory
  if (!fs.existsSync(CONFIG.outputDir)) {
    fs.mkdirSync(CONFIG.outputDir, { recursive: true });
  }

  // ====== LOAD STREETS ======
  console.log('Loading streets from DB...');
  const streetsResult = await client.query(`
    SELECT id, geo_id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM streets WHERE geo_id IS NOT NULL
  `);

  const streetNameMap = new Map();
  for (const s of streetsResult.rows) {
    const street = {
      id: s.id,
      geo_id: s.geo_id,
      nameUk: s.name_uk,
      nameRu: s.name_ru
    };
    const normUk = normalizeStreetName(s.name_uk);
    const normRu = normalizeStreetName(s.name_ru);
    if (normUk && normUk.length >= 3) streetNameMap.set(normUk, street);
    if (normRu && normRu.length >= 3) streetNameMap.set(normRu, street);
  }
  console.log(`Loaded ${streetNameMap.size} unique street names\n`);

  // ====== GET TEST LISTINGS ======
  console.log(`Fetching ${CONFIG.testLimit} OLX listings without street_id...`);
  const listingsResult = await client.query(`
    SELECT id, description->>'uk' as desc_uk, description->>'ru' as desc_ru
    FROM unified_listings
    WHERE realty_platform = 'olx'
      AND street_id IS NULL
    ORDER BY RANDOM()
    LIMIT $1
  `, [CONFIG.testLimit]);

  const listings = listingsResult.rows;
  console.log(`Got ${listings.length} listings for testing\n`);

  // ====== PROCESS ======
  console.log('Starting GPT extraction...');
  const startTime = Date.now();
  const allResults = [];

  // Process in batches
  const batchSize = CONFIG.concurrency;
  for (let i = 0; i < listings.length; i += batchSize) {
    const batch = listings.slice(i, i + batchSize);

    // Process batch concurrently
    const batchPromises = batch.map(listing =>
      (async () => {
        const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

        if (!description) {
          return {
            id: listing.id,
            status: 'empty_description',
            description: '',
            gptResult: null,
            matchResult: null
          };
        }

        const gptResult = await extractStreetGPT(openai, description);

        let matchResult = null;
        let status = 'unknown';

        if (!gptResult.success) {
          status = 'api_error';
        } else if (!gptResult.street) {
          status = 'no_street_found';
        } else {
          matchResult = findBestStreetMatch(gptResult.street, streetNameMap);
          status = matchResult ? 'matched' : 'no_match_in_db';
        }

        return {
          id: listing.id,
          status,
          description: description.slice(0, 500),
          gptResult: {
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
            dbStreetName: matchResult.match.nameUk || matchResult.match.nameRu
          } : null
        };
      })()
    );

    const batchResults = await Promise.all(batchPromises);
    allResults.push(...batchResults);

    // Progress
    const progress = Math.min(i + batchSize, listings.length);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const matched = allResults.filter(r => r.status === 'matched').length;
    const noMatch = allResults.filter(r => r.status === 'no_match_in_db').length;
    const noStreet = allResults.filter(r => r.status === 'no_street_found').length;

    console.log(`Progress: ${progress}/${listings.length} | Matched: ${matched} | NoMatchDB: ${noMatch} | NoStreet: ${noStreet} | ${elapsed}s`);

    // Delay between batches
    if (i + batchSize < listings.length) {
      await new Promise(r => setTimeout(r, CONFIG.delayBetweenBatches));
    }
  }

  // ====== CALCULATE STATS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  const stats = {
    total: allResults.length,
    matched: allResults.filter(r => r.status === 'matched').length,
    noMatchInDb: allResults.filter(r => r.status === 'no_match_in_db').length,
    noStreetFound: allResults.filter(r => r.status === 'no_street_found').length,
    emptyDescription: allResults.filter(r => r.status === 'empty_description').length,
    apiError: allResults.filter(r => r.status === 'api_error').length,
  };

  stats.matchedPercent = ((stats.matched / stats.total) * 100).toFixed(1);
  stats.noMatchPercent = ((stats.noMatchInDb / stats.total) * 100).toFixed(1);
  stats.noStreetPercent = ((stats.noStreetFound / stats.total) * 100).toFixed(1);
  stats.effectiveRate = (((stats.matched + stats.noMatchInDb) / stats.total) * 100).toFixed(1);

  // Token stats
  const successResults = allResults.filter(r => r.gptResult?.tokens);
  const totalTokens = successResults.reduce((sum, r) => sum + (r.gptResult?.tokens?.total_tokens || 0), 0);
  const avgLatency = successResults.reduce((sum, r) => sum + (r.gptResult?.latency || 0), 0) / successResults.length;

  stats.totalTokens = totalTokens;
  stats.avgTokensPerRequest = Math.round(totalTokens / successResults.length);
  stats.avgLatencyMs = Math.round(avgLatency);
  stats.estimatedCost = (totalTokens / 1000 * 0.0005).toFixed(4); // GPT-3.5-turbo pricing
  stats.processingTime = totalTime;

  // Match type breakdown
  const matchedResults = allResults.filter(r => r.status === 'matched');
  stats.matchTypes = {
    exact: matchedResults.filter(r => r.matchResult?.matchType === 'exact').length,
    fuzzy: matchedResults.filter(r => r.matchResult?.matchType === 'fuzzy').length,
    substring: matchedResults.filter(r => r.matchResult?.matchType === 'substring').length
  };

  // ====== SAVE RESULTS ======
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

  // All results
  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-results-${timestamp}.json`),
    JSON.stringify(allResults, null, 2)
  );

  // Summary
  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-summary-${timestamp}.json`),
    JSON.stringify(stats, null, 2)
  );

  // No match in DB (for analysis)
  const noMatchResults = allResults.filter(r => r.status === 'no_match_in_db');
  fs.writeFileSync(
    path.join(CONFIG.outputDir, `gpt-test-no-match-${timestamp}.json`),
    JSON.stringify(noMatchResults, null, 2)
  );

  // ====== PRINT SUMMARY ======
  console.log('\n' + '='.repeat(60));
  console.log('РЕЗУЛЬТАТЫ ТЕСТА');
  console.log('='.repeat(60));
  console.log(`\nВсего обработано: ${stats.total}`);
  console.log(`Время обработки: ${stats.processingTime}s`);
  console.log(`\n--- СТАТУСЫ ---`);
  console.log(`✅ Matched (найдено в БД):     ${stats.matched} (${stats.matchedPercent}%)`);
  console.log(`⚠️  No Match in DB:            ${stats.noMatchInDb} (${stats.noMatchPercent}%)`);
  console.log(`❌ No Street Found (GPT null): ${stats.noStreetFound} (${stats.noStreetPercent}%)`);
  console.log(`📭 Empty Description:          ${stats.emptyDescription}`);
  console.log(`🔴 API Errors:                 ${stats.apiError}`);
  console.log(`\n--- ЭФФЕКТИВНОСТЬ ---`);
  console.log(`GPT нашёл улицу: ${stats.effectiveRate}% (${stats.matched + stats.noMatchInDb} из ${stats.total})`);
  console.log(`Успешный матчинг: ${stats.matchedPercent}%`);
  console.log(`\n--- ТИПЫ МАТЧИНГА ---`);
  console.log(`Exact match:     ${stats.matchTypes.exact}`);
  console.log(`Fuzzy match:     ${stats.matchTypes.fuzzy}`);
  console.log(`Substring match: ${stats.matchTypes.substring}`);
  console.log(`\n--- СТОИМОСТЬ ---`);
  console.log(`Всего токенов: ${stats.totalTokens}`);
  console.log(`Среднее на запрос: ${stats.avgTokensPerRequest} токенов`);
  console.log(`Средняя задержка: ${stats.avgLatencyMs}ms`);
  console.log(`Стоимость теста: ~$${stats.estimatedCost}`);
  console.log(`\n--- ЭКСТРАПОЛЯЦИЯ НА 316k ---`);
  const extrapolatedCost = (316000 * stats.avgTokensPerRequest / 1000 * 0.0005).toFixed(2);
  const extrapolatedTime = ((316000 / stats.total) * parseFloat(stats.processingTime) / 60).toFixed(0);
  console.log(`Ожидаемая стоимость: ~$${extrapolatedCost}`);
  console.log(`Ожидаемое время: ~${extrapolatedTime} минут`);
  console.log(`Ожидаемый результат: ~${Math.round(316000 * stats.matched / stats.total)} новых street_id`);

  console.log(`\n--- ФАЙЛЫ ---`);
  console.log(`Все результаты: ${CONFIG.outputDir}/gpt-test-results-${timestamp}.json`);
  console.log(`Статистика: ${CONFIG.outputDir}/gpt-test-summary-${timestamp}.json`);
  console.log(`Не найдено в БД: ${CONFIG.outputDir}/gpt-test-no-match-${timestamp}.json`);

  // Print some examples of no-match
  if (noMatchResults.length > 0) {
    console.log(`\n--- ПРИМЕРЫ "NO MATCH IN DB" (первые 10) ---`);
    noMatchResults.slice(0, 10).forEach((r, i) => {
      console.log(`${i+1}. GPT: "${r.gptResult?.extractedStreet}"`);
      console.log(`   Описание: ${r.description?.slice(0, 100)}...`);
    });
  }

  await client.end();
  console.log('\n✅ Тест завершён');
}

main().catch(console.error);
