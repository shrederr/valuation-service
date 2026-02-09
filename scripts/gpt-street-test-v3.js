/**
 * GPT Street Recognition Test v3
 *
 * Улучшения:
 * 1. Двойной GPT запрос: если не нашли улицу, спрашиваем актуальное название
 * 2. Поиск улицы в радиусе 5км от координат
 * 3. Те же объекты из Одессы что и в v2
 */

const { Client } = require('pg');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');

// ====== CONFIG ======
const CONFIG = {
  testLimit: 100,
  concurrency: 5, // уменьшил из-за двойных запросов
  delayBetweenBatches: 1500,
  maxDescriptionLength: 1500,
  searchRadiusKm: 5,
  outputDir: path.join(__dirname, '../output/gpt-test-v3'),
  odessa: {
    latMin: 46.3,
    latMax: 46.6,
    lngMin: 30.6,
    lngMax: 30.9
  }
};

const SYSTEM_PROMPT_EXTRACT = `Ты помощник для извлечения адресов из украинских объявлений о недвижимости.

ЗАДАЧА: Извлеки населённый пункт и улицу из текста.

ПРАВИЛА:
1. Улицу в именительном падеже, без префиксов (вул., ул., просп., пр-т, б-р, пров., пер.)
2. Если не указаны явно - ставь null
3. ЖК - это НЕ улица
4. Районы (Черёмушки, Таирова, Аркадия, Поскот) - это НЕ улицы
5. Станции Фонтана, номера микрорайонов - это НЕ улицы
6. Если указаны две улицы через "/" - бери первую

ФОРМАТ ОТВЕТА (только JSON): {"settlement": "...", "street": "..."}

ПРИМЕРЫ:
"вул. Хрещатик 15, Київ" → {"settlement": "київ", "street": "хрещатик"}
"Черёмушки, 5 этаж" → {"settlement": null, "street": null}
"ул. Ильфа и Петрова/Вильямса" → {"settlement": null, "street": "ільфа і петрова"}
"10 станція Фонтану" → {"settlement": null, "street": null}`;

const SYSTEM_PROMPT_RENAME = `Ты эксперт по переименованиям улиц в Украине.

ЗАДАЧА: Определи актуальное (современное) название улицы.

Многие улицы в Украине были переименованы в рамках декоммунизации или по другим причинам.
Если это старое название - верни текущее официальное название.
Если название актуальное или ты не знаешь о переименовании - верни то же самое.

ФОРМАТ ОТВЕТА (только JSON): {"currentName": "актуальное название улицы"}

ПРИМЕРЫ:
Вход: "улица Ленина, Одесса" → {"currentName": "дерибасівська"}
Вход: "Дворянська, Одеса" → {"currentName": "всеволода змієнка"}
Вход: "Французький бульвар, Одеса" → {"currentName": "французький бульвар"}
Вход: "Комсомольська, Київ" → {"currentName": "алли горської"}`;

// ====== NORMALIZATION ======
function normalizeStreetName(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    // Убираем PREFIX
    .replace(/^(вулиця|вул\.|вул|улица|ул\.|ул|проспект|просп\.|пр-т|пр\.|пр|провулок|пров\.|переулок|пер\.|бульвар|бульв\.|б-р|площа|пл\.|площадь|набережна|наб\.|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)\s*/gi, '')
    // Убираем SUFFIX
    .replace(/\s+(вулиця|улица|проспект|провулок|переулок|бульвар|площа|площадь|набережна|шосе|шоссе|алея|проїзд|проезд|узвіз|спуск|тупик|майдан)$/gi, '')
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
        { role: 'system', content: SYSTEM_PROMPT_EXTRACT },
        { role: 'user', content: description.slice(0, CONFIG.maxDescriptionLength) }
      ],
      temperature: 0,
      max_tokens: 100,
      response_format: { type: 'json_object' }
    });

    const content = response.choices[0].message.content;
    const parsed = JSON.parse(content);

    return {
      success: true,
      settlement: parsed.settlement ? normalizeSettlement(parsed.settlement) : null,
      street: parsed.street ? normalizeStreetName(parsed.street) : null,
      rawResponse: content,
      tokens: response.usage,
      latency: Date.now() - startTime
    };
  } catch (error) {
    return { success: false, error: error.message, latency: Date.now() - startTime };
  }
}

async function getCurrentStreetName(openai, oldStreet, settlement) {
  const startTime = Date.now();
  try {
    const query = settlement
      ? `${oldStreet}, ${settlement}`
      : `${oldStreet}, Одеса`;

    const response = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [
        { role: 'system', content: SYSTEM_PROMPT_RENAME },
        { role: 'user', content: query }
      ],
      temperature: 0,
      max_tokens: 50,
      response_format: { type: 'json_object' }
    });

    const content = response.choices[0].message.content;
    const parsed = JSON.parse(content);

    return {
      success: true,
      currentName: parsed.currentName ? normalizeStreetName(parsed.currentName) : null,
      rawResponse: content,
      tokens: response.usage,
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

  // 2. Fuzzy match (Levenshtein ≤ 2)
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
  if (!process.env.OPENAI_API_KEY) {
    console.log('⚠️  OPENAI_API_KEY не установлен!');
    return;
  }

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== GPT Street Recognition Test v3 ===');
  console.log('Логика: GPT extract → match → [если нет] GPT rename → match\n');

  if (!fs.existsSync(CONFIG.outputDir)) {
    fs.mkdirSync(CONFIG.outputDir, { recursive: true });
  }

  // ====== GET SAME ODESSA LISTINGS ======
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
  console.log('Processing with double-GPT for old street names...\n');
  const startTime = Date.now();
  const allResults = [];

  let totalTokens = 0;
  let renameAttempts = 0;
  let renameSuccess = 0;

  for (let i = 0; i < listings.length; i += CONFIG.concurrency) {
    const batch = listings.slice(i, i + CONFIG.concurrency);

    const batchPromises = batch.map(async (listing) => {
      const description = ((listing.desc_uk || '') + ' ' + (listing.desc_ru || '')).trim();

      if (!description) {
        return {
          id: listing.id, lat: listing.lat, lng: listing.lng,
          status: 'empty_description', gptExtract: null, gptRename: null, matchResult: null
        };
      }

      // Step 1: Extract address
      const extractResult = await extractAddressGPT(openai, description);
      totalTokens += extractResult.tokens?.total_tokens || 0;

      if (!extractResult.success) {
        return {
          id: listing.id, lat: listing.lat, lng: listing.lng, status: 'api_error',
          description: description.slice(0, 300),
          gptExtract: extractResult, gptRename: null, matchResult: null
        };
      }

      if (!extractResult.street) {
        return {
          id: listing.id, lat: listing.lat, lng: listing.lng, status: 'no_street_found',
          description: description.slice(0, 300),
          gptExtract: extractResult, gptRename: null, matchResult: null
        };
      }

      // Get nearby streets
      const nearbyStreetsResult = await client.query(`
        SELECT s.id, s.geo_id, s.name->>'uk' as name_uk, s.name->>'ru' as name_ru,
               ST_Distance(ST_Centroid(s.line)::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) / 1000 as distance_km
        FROM streets s
        WHERE ST_DWithin(ST_Centroid(s.line)::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
        ORDER BY distance_km
      `, [listing.lng, listing.lat, CONFIG.searchRadiusKm * 1000]);

      const nearbyStreets = nearbyStreetsResult.rows;

      // Step 2: Try to match extracted street
      let matchResult = findBestStreetMatch(extractResult.street, nearbyStreets);
      let gptRenameResult = null;
      let finalStatus = 'unknown';

      if (matchResult) {
        finalStatus = 'matched_direct';
      } else {
        // Step 3: No match - ask GPT for current name
        renameAttempts++;
        gptRenameResult = await getCurrentStreetName(openai, extractResult.street, extractResult.settlement);
        totalTokens += gptRenameResult.tokens?.total_tokens || 0;

        if (gptRenameResult.success && gptRenameResult.currentName &&
            gptRenameResult.currentName !== extractResult.street) {
          // Try matching with new name
          matchResult = findBestStreetMatch(gptRenameResult.currentName, nearbyStreets);

          if (matchResult) {
            finalStatus = 'matched_after_rename';
            renameSuccess++;
          } else {
            finalStatus = 'no_match_after_rename';
          }
        } else {
          finalStatus = 'no_match_same_name';
        }
      }

      return {
        id: listing.id,
        lat: listing.lat,
        lng: listing.lng,
        status: finalStatus,
        description: description.slice(0, 300),
        nearbyStreetsCount: nearbyStreets.length,
        gptExtract: {
          settlement: extractResult.settlement,
          street: extractResult.street,
          tokens: extractResult.tokens,
          latency: extractResult.latency
        },
        gptRename: gptRenameResult ? {
          originalStreet: extractResult.street,
          currentName: gptRenameResult.currentName,
          tokens: gptRenameResult.tokens,
          latency: gptRenameResult.latency
        } : null,
        matchResult: matchResult ? {
          streetId: matchResult.match.id,
          geoId: matchResult.match.geo_id,
          matchType: matchResult.type,
          dbStreetName: matchResult.match.name_uk || matchResult.match.name_ru,
          distanceKm: matchResult.distance
        } : null
      };
    });

    const batchResults = await Promise.all(batchPromises);
    allResults.push(...batchResults);

    // Progress
    const progress = Math.min(i + CONFIG.concurrency, listings.length);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const matchedDirect = allResults.filter(r => r.status === 'matched_direct').length;
    const matchedRename = allResults.filter(r => r.status === 'matched_after_rename').length;
    const noStreet = allResults.filter(r => r.status === 'no_street_found').length;

    console.log(`Progress: ${progress}/${listings.length} | Direct: ${matchedDirect} | AfterRename: ${matchedRename} | NoStreet: ${noStreet} | ${elapsed}s`);

    if (i + CONFIG.concurrency < listings.length) {
      await new Promise(r => setTimeout(r, CONFIG.delayBetweenBatches));
    }
  }

  // ====== STATS ======
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);

  const stats = {
    total: allResults.length,
    matchedDirect: allResults.filter(r => r.status === 'matched_direct').length,
    matchedAfterRename: allResults.filter(r => r.status === 'matched_after_rename').length,
    noMatchAfterRename: allResults.filter(r => r.status === 'no_match_after_rename').length,
    noMatchSameName: allResults.filter(r => r.status === 'no_match_same_name').length,
    noStreetFound: allResults.filter(r => r.status === 'no_street_found').length,
    emptyDescription: allResults.filter(r => r.status === 'empty_description').length,
    apiError: allResults.filter(r => r.status === 'api_error').length,
  };

  stats.totalMatched = stats.matchedDirect + stats.matchedAfterRename;
  stats.totalMatchedPercent = ((stats.totalMatched / stats.total) * 100).toFixed(1);
  stats.matchedDirectPercent = ((stats.matchedDirect / stats.total) * 100).toFixed(1);
  stats.matchedAfterRenamePercent = ((stats.matchedAfterRename / stats.total) * 100).toFixed(1);

  stats.renameAttempts = renameAttempts;
  stats.renameSuccess = renameSuccess;
  stats.renameSuccessRate = renameAttempts > 0 ? ((renameSuccess / renameAttempts) * 100).toFixed(1) : 0;

  stats.totalTokens = totalTokens;
  stats.estimatedCost = (totalTokens / 1000 * 0.0005).toFixed(4);
  stats.processingTime = totalTime;

  // ====== SAVE ======
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

  fs.writeFileSync(path.join(CONFIG.outputDir, `results-${timestamp}.json`), JSON.stringify(allResults, null, 2));
  fs.writeFileSync(path.join(CONFIG.outputDir, `summary-${timestamp}.json`), JSON.stringify(stats, null, 2));

  // Rename examples
  const renameExamples = allResults.filter(r => r.gptRename && r.gptRename.currentName !== r.gptExtract?.street);
  fs.writeFileSync(path.join(CONFIG.outputDir, `rename-examples-${timestamp}.json`), JSON.stringify(renameExamples, null, 2));

  // ====== PRINT ======
  console.log('\n' + '='.repeat(60));
  console.log('РЕЗУЛЬТАТЫ ТЕСТА v3 (с переименованием улиц)');
  console.log('='.repeat(60));
  console.log(`\nВсего: ${stats.total} | Время: ${stats.processingTime}s`);
  console.log(`\n--- МАТЧИНГ ---`);
  console.log(`✅ Direct match:        ${stats.matchedDirect} (${stats.matchedDirectPercent}%)`);
  console.log(`✅ After rename:        ${stats.matchedAfterRename} (${stats.matchedAfterRenamePercent}%)`);
  console.log(`📊 ИТОГО matched:       ${stats.totalMatched} (${stats.totalMatchedPercent}%)`);
  console.log(`\n--- НЕ НАЙДЕНО ---`);
  console.log(`❌ No match (renamed):  ${stats.noMatchAfterRename}`);
  console.log(`❌ No match (same):     ${stats.noMatchSameName}`);
  console.log(`❌ No street in text:   ${stats.noStreetFound}`);
  console.log(`\n--- ПЕРЕИМЕНОВАНИЕ ---`);
  console.log(`Попыток rename:         ${stats.renameAttempts}`);
  console.log(`Успешных rename→match:  ${stats.renameSuccess} (${stats.renameSuccessRate}%)`);
  console.log(`\n--- СТОИМОСТЬ ---`);
  console.log(`Токенов: ${stats.totalTokens}`);
  console.log(`Стоимость: ~$${stats.estimatedCost}`);

  // Examples of successful renames
  const successfulRenames = allResults.filter(r => r.status === 'matched_after_rename');
  if (successfulRenames.length > 0) {
    console.log(`\n--- ПРИМЕРЫ УСПЕШНОГО ПЕРЕИМЕНОВАНИЯ ---`);
    successfulRenames.slice(0, 10).forEach((r, i) => {
      console.log(`${i+1}. "${r.gptExtract?.street}" → "${r.gptRename?.currentName}" → DB: "${r.matchResult?.dbStreetName}"`);
    });
  }

  console.log(`\n--- ФАЙЛЫ ---`);
  console.log(`${CONFIG.outputDir}/`);

  await client.end();
  console.log('\n✅ Тест завершён');
}

main().catch(console.error);
