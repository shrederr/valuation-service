/**
 * Complete re-matching of v2 streets → our streets.
 * v3: name-first matching with region-wide search
 *
 * Strategy:
 * 1. Build global name index: normalized_name → [{street, geo, region}]
 * 2. For each v2 street: find by name, then verify geo compatibility
 * 3. Prefer exact geo match, then same city, then same region
 */
const {Client} = require('pg');
const fs = require('fs');
const path = require('path');

// ===== UTILS =====
function parseCSV(filePath) {
  const lines = fs.readFileSync(filePath, 'utf-8').split('\n').filter(l => l.trim());
  const headers = lines[0].split(',');
  return lines.slice(1).filter(l => !l.startsWith('(')).map(l => {
    const vals = [];
    let cur = '';
    let inQuotes = false;
    for (const ch of l) {
      if (ch === '"') { inQuotes = !inQuotes; continue; }
      if (ch === ',' && !inQuotes) { vals.push(cur); cur = ''; continue; }
      cur += ch;
    }
    vals.push(cur);
    const row = {};
    headers.forEach((h, i) => row[h] = (vals[i] || '').trim());
    return row;
  });
}

function normalizeStreet(name) {
  if (!name) return '';
  return name
    .replace(/\s*\(.*?\)\s*/g, ' ')
    .replace(/(улица|вулиця|вулицi|переулок|провулок|проспект|бульвар|набережная|набережна|площадь|площа|тупик|тупік|шоссе|шосе|дорога|проезд|проїзд|аллея|алея|линия|лінія|спуск|въезд|в'їзд|мікрорайон|микрорайон|массив|масив|урочище|шлях|сквер|парк|квартал|роз'їзд|розїзд|разъезд|узвіз)/gi, ' ')
    .replace(/(ул\.|вул\.|пер\.|пров\.|просп\.|бульв\.|бул\.|наб\.|пл\.|дор\.|мкр\.|м-н|ак\.|марш\.|ген\.|адм\.|гетьм\.|пр\.|кап\.|проф\.|атам\.|отам\.|сп\.|туп\.|шос\.|лейт\.|вице-?адм\.?)/gi, ' ')
    .replace(/(академика|академіка|генерала|маршала|адмирала|адмірала|капитана|капітана|профессора|професора|атамана|отамана|гетмана|лейтенанта|полковника|сержанта|космонавта|героя|героїв|герои|святого|святої|князя|княгини|княгині)/gi, ' ')
    .replace(/ и /g, ' ').replace(/ та /g, ' ').replace(/ і /g, ' ')
    .replace(/ё/g, 'е').replace(/Ё/g, 'Е')
    .replace(/ї/g, 'і').replace(/Ї/g, 'І')
    .replace(/є/g, 'е').replace(/Є/g, 'Е')
    .replace(/ґ/g, 'г').replace(/Ґ/g, 'Г')
    .replace(/['ʼ`'"«»""ʼ']/g, '')
    .replace(/№/g, '')
    .replace(/\s+/g, ' ')
    .trim().toLowerCase();
}

function normalizeStreetCross(name) {
  return normalizeStreet(name)
    .replace(/і/g, 'е')
    .replace(/ь/g, '')
    .replace(/й/g, 'и')
    .replace(/ы/g, 'и')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ').trim();
}

function extractStreetNames(nameRu, nameUa, {includeSurname = false} = {}) {
  const names = new Set();
  for (const name of [nameRu, nameUa]) {
    if (!name) continue;
    const n1 = normalizeStreet(name);
    const n2 = normalizeStreetCross(name);
    if (n1 && n1.length >= 2) names.add(n1);
    if (n2 && n2.length >= 2) names.add(n2);
    // Extract alternate names from parentheses
    const pm = name.match(/\(([^)]+)\)/);
    if (pm) {
      const alt = pm[1].split(/[,;]/).map(s => s.trim()).filter(Boolean);
      for (const a of alt) {
        const na = normalizeStreet(a);
        const nc = normalizeStreetCross(a);
        if (na && na.length >= 2) names.add(na);
        if (nc && nc.length >= 2) names.add(nc);
      }
      const base = name.replace(/\s*\(.*?\)\s*/, '').trim();
      const nb = normalizeStreet(base);
      const nbc = normalizeStreetCross(base);
      if (nb && nb.length >= 2) names.add(nb);
      if (nbc && nbc.length >= 2) names.add(nbc);
    }
    // Reversed 2-word names: "Франко Ивана" → "ивана франко"
    const words1 = n1.split(/\s+/).filter(w => w.length >= 2);
    if (words1.length === 2) names.add(words1[1] + ' ' + words1[0]);
    const words2 = n2.split(/\s+/).filter(w => w.length >= 2);
    if (words2.length === 2) names.add(words2[1] + ' ' + words2[0]);
    // Strip initials (1-2 char words like "Т.", "Дж.", "И.") and match remainder
    const wordsClean1 = n1.split(/\s+/).filter(w => w.length >= 3);
    if (wordsClean1.length >= 1 && wordsClean1.length < words1.length) {
      names.add(wordsClean1.join(' '));
      if (wordsClean1.length === 2) names.add(wordsClean1[1] + ' ' + wordsClean1[0]);
    }
    const wordsClean2 = n2.split(/\s+/).filter(w => w.length >= 3);
    if (wordsClean2.length >= 1 && wordsClean2.length < words2.length) {
      names.add(wordsClean2.join(' '));
      if (wordsClean2.length === 2) names.add(wordsClean2[1] + ' ' + wordsClean2[0]);
    }
    // Surname-only: for multi-word names, add each individual word as a potential match
    // This handles cases like v2:"Пулюя" matching our:"Ивана Пулюя"
    // Skip for dual-person names (containing conjunctions: "та", "і", "и")
    const nameLower = name.toLowerCase();
    const hasConjunction = / та | і /i.test(nameLower) || / и /i.test(nameLower);
    if (!hasConjunction || includeSurname) {
      const allWords = n1.split(/\s+/).filter(w => w.length >= 4);
      if (allWords.length >= 2) {
        for (const word of allWords) {
          if (word.length >= 5) names.add(word);
        }
      }
      // Also for cross-normalized
      const allWords2 = n2.split(/\s+/).filter(w => w.length >= 4);
      if (allWords2.length >= 2) {
        for (const word of allWords2) {
          if (word.length >= 5) names.add(word);
        }
      }
    }
  }
  names.delete('');
  return [...names].filter(n => n.length >= 2);
}

function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const d = Array.from({length: m + 1}, (_, i) => {
    const row = new Array(n + 1);
    row[0] = i;
    return row;
  });
  for (let j = 1; j <= n; j++) d[0][j] = j;
  for (let i = 1; i <= m; i++)
    for (let j = 1; j <= n; j++)
      d[i][j] = Math.min(
        d[i-1][j] + 1, d[i][j-1] + 1,
        d[i-1][j-1] + (a[i-1] === b[j-1] ? 0 : 1)
      );
  return d[m][n];
}

function similarity(a, b) {
  if (!a || !b) return 0;
  const maxLen = Math.max(a.length, b.length);
  if (maxLen === 0) return 1;
  return 1 - levenshtein(a, b) / maxLen;
}

// ===== MAIN =====
async function main() {
  const c = new Client({host:'localhost',port:5433,user:'postgres',password:'postgres',database:'valuation'});
  await c.connect();

  console.log('=== ЗАГРУЗКА ДАННЫХ ===\n');

  // Load v2 streets
  const v2Streets = parseCSV(path.join(__dirname, '..', 'data', 'vector2_streets.csv'));
  const v2Usage = parseCSV(path.join(__dirname, '..', 'data', 'vector2_street_usage.csv'));
  const v2UsageMap = {};
  for (const u of v2Usage) v2UsageMap[u.geo_street] = parseInt(u.cnt);
  console.log(`v2 улиц: ${v2Streets.length}, с объектами: ${v2Usage.length}`);

  const v2ById = {};
  for (const s of v2Streets) {
    v2ById[parseInt(s.kod)] = {
      id: parseInt(s.kod), nameRu: s.name_ru, nameUa: s.name_ua,
      geoId: parseInt(s.fk_geoid), alias: s.alias,
      objects: v2UsageMap[s.kod] || 0
    };
  }

  // Load geo mappings
  const geoMappings = await c.query("SELECT source_id, local_id FROM source_id_mappings WHERE source='vector2_crm' AND entity_type='geo'");
  const v2GeoToOurGeo = {};
  for (const m of geoMappings.rows) v2GeoToOurGeo[m.source_id] = m.local_id;
  console.log(`Geo маппингов: ${geoMappings.rows.length}`);

  // Load our geo tree
  const ourGeos = await c.query("SELECT id, name->>'ru' as name_ru, name->>'uk' as name_uk, type, parent_id FROM geo");
  const ourGeoById = {};
  for (const g of ourGeos.rows) ourGeoById[g.id] = g;

  const KYIV_CITY_ID = 27132;
  const KYIV_REGION_ID = 19338;

  function getRegionId(geoId) {
    const visited = new Set();
    let cur = ourGeoById[geoId];
    for (let i = 0; i < 15 && cur; i++) {
      if (visited.has(cur.id)) break;
      visited.add(cur.id);
      if (cur.type === 'region') return cur.id;
      if (cur.id === KYIV_CITY_ID) return KYIV_REGION_ID;
      cur = cur.parent_id ? ourGeoById[cur.parent_id] : null;
    }
    return null;
  }

  function getCityId(geoId) {
    const visited = new Set();
    let cur = ourGeoById[geoId];
    for (let i = 0; i < 10 && cur; i++) {
      if (visited.has(cur.id)) break;
      visited.add(cur.id);
      if (cur.type === 'city') return cur.id;
      if (cur.id === KYIV_CITY_ID) return KYIV_CITY_ID;
      cur = cur.parent_id ? ourGeoById[cur.parent_id] : null;
    }
    return null;
  }

  // Load our streets and build indices
  const ourStreets = await c.query("SELECT id, name->>'ru' as name_ru, name->>'uk' as name_uk, geo_id FROM streets");
  console.log(`Наших улиц: ${ourStreets.rows.length}`);

  // Build name → street index per region
  // nameIndex[regionId][normName] = [{street, geoId, cityId}]
  const nameIndex = {};
  let streetsIndexed = 0;

  for (const s of ourStreets.rows) {
    const regionId = getRegionId(s.geo_id);
    if (!regionId) continue;
    if (!nameIndex[regionId]) nameIndex[regionId] = {};

    const normNames = extractStreetNames(s.name_ru, s.name_uk);
    const cityId = getCityId(s.geo_id);

    for (const name of normNames) {
      if (!nameIndex[regionId][name]) nameIndex[regionId][name] = [];
      nameIndex[regionId][name].push({id: s.id, geoId: s.geo_id, cityId, name_ru: s.name_ru, name_uk: s.name_uk});
    }
    streetsIndexed++;
  }
  console.log(`Индексировано улиц: ${streetsIndexed}`);

  // Also build fuzzy index: for each region, normalized names for fuzzy search
  // regionStreets[regionId] = [{id, geoId, cityId, normNames: [...]}]
  const regionStreets = {};
  for (const s of ourStreets.rows) {
    const regionId = getRegionId(s.geo_id);
    if (!regionId) continue;
    if (!regionStreets[regionId]) regionStreets[regionId] = [];
    const normNames = extractStreetNames(s.name_ru, s.name_uk);
    const cityId = getCityId(s.geo_id);
    regionStreets[regionId].push({id: s.id, geoId: s.geo_id, cityId, normNames, name_ru: s.name_ru, name_uk: s.name_uk});
  }

  // ===== LOAD RENAME TABLE =====
  const renameData = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data', 'street_renames.json'), 'utf-8'));
  // Build lookup: normalized old name → [new name variants]
  const renameTable = {};
  for (const r of renameData.renames) {
    const oldNames = extractStreetNames(r.old, r.old, {includeSurname: false});
    const newNames = extractStreetNames(r.new, r.new, {includeSurname: false});
    for (const on of oldNames) {
      if (!renameTable[on]) renameTable[on] = [];
      renameTable[on].push(...newNames);
    }
  }
  console.log(`Rename table: ${renameData.renames.length} entries, ${Object.keys(renameTable).length} old name variants`);

  // ===== MATCHING =====
  console.log('\n=== МАТЧИНГ УЛИЦ ===\n');

  const results = [];
  const unmatched = [];
  const skippedNoGeoMapping = [];

  const v2All = Object.values(v2ById).sort((a,b) => b.objects - a.objects);

  for (const v2 of v2All) {
    const ourGeoId = v2GeoToOurGeo[v2.geoId];
    if (!ourGeoId) {
      skippedNoGeoMapping.push(v2);
      continue;
    }

    const ourRegionId = getRegionId(ourGeoId);
    if (!ourRegionId) { skippedNoGeoMapping.push(v2); continue; }

    const ourCityId = getCityId(ourGeoId);
    const v2Names = extractStreetNames(v2.nameRu, v2.nameUa, {includeSurname: true});
    const regionNameIndex = nameIndex[ourRegionId] || {};

    let bestMatch = null;
    let bestMethod = null;
    let bestConf = 0;
    let bestPriority = 999; // lower = better geo match

    // Method 1: Exact name match with geo priority
    for (const name of v2Names) {
      const candidates = regionNameIndex[name];
      if (!candidates) continue;

      for (const cand of candidates) {
        let priority;
        if (cand.geoId === ourGeoId) priority = 1;          // Same geo
        else if (cand.cityId === ourCityId) priority = 2;    // Same city
        else if (cand.cityId === ourGeoId) priority = 2;     // Our geo is city, street in sub-district
        else priority = 3;                                    // Same region, different area

        const conf = priority <= 2 ? 1.0 : 0.90;  // Lower conf for cross-city matches

        if (priority < bestPriority || (priority === bestPriority && conf > bestConf)) {
          bestMatch = cand; bestMethod = 'exact_name'; bestConf = conf; bestPriority = priority;
        }
      }
      if (bestPriority === 1) break; // Best possible match
    }

    // Method 1.5: Rename-based match — old Soviet name → new Ukrainian name
    if (!bestMatch) {
      for (const name of v2Names) {
        const newNames = renameTable[name];
        if (!newNames) continue;
        for (const newName of newNames) {
          const candidates = regionNameIndex[newName];
          if (!candidates) continue;
          for (const cand of candidates) {
            let priority;
            if (cand.geoId === ourGeoId) priority = 1;
            else if (cand.cityId === ourCityId) priority = 2;
            else if (cand.cityId === ourGeoId) priority = 2;
            else priority = 3;
            const conf = priority <= 2 ? 0.95 : 0.85;
            if (priority < bestPriority || (priority === bestPriority && conf > bestConf)) {
              bestMatch = cand; bestMethod = 'renamed'; bestConf = conf; bestPriority = priority;
            }
          }
          if (bestPriority === 1) break;
        }
        if (bestPriority === 1) break;
      }
    }

    // Method 2: Fuzzy match within same city (sim ≥ 0.90)
    if (!bestMatch && ourCityId) {
      let bestSim = 0;
      const cityStreets = (regionStreets[ourRegionId] || []).filter(s =>
        s.cityId === ourCityId || s.geoId === ourGeoId
      );
      for (const name of v2Names) {
        if (!name || name.length < 4) continue;
        for (const cand of cityStreets) {
          for (const cn of cand.normNames) {
            if (!cn || cn.length < 4) continue;
            if (Math.abs(name.length - cn.length) > Math.max(name.length, cn.length) * 0.3) continue;
            const sim = similarity(name, cn);
            if (sim >= 0.90 && sim > bestSim) {
              bestMatch = cand; bestMethod = 'fuzzy_city'; bestConf = sim; bestSim = sim;
            }
          }
        }
      }
    }

    // Method 3: Fuzzy match within region (sim ≥ 0.92, stricter)
    if (!bestMatch) {
      let bestSim = 0;
      const allRegionStreets = regionStreets[ourRegionId] || [];
      for (const name of v2Names) {
        if (!name || name.length < 5) continue;
        for (const cand of allRegionStreets) {
          for (const cn of cand.normNames) {
            if (!cn || cn.length < 5) continue;
            if (Math.abs(name.length - cn.length) > Math.max(name.length, cn.length) * 0.25) continue;
            const sim = similarity(name, cn);
            if (sim >= 0.92 && sim > bestSim) {
              bestMatch = cand; bestMethod = 'fuzzy_region'; bestConf = sim; bestSim = sim;
            }
          }
        }
      }
    }

    if (bestMatch) {
      results.push({
        v2Id: v2.id, ourId: bestMatch.id, method: bestMethod, confidence: bestConf,
        v2, ourName: bestMatch.name_ru || bestMatch.name_uk, priority: bestPriority
      });
    } else {
      unmatched.push(v2);
    }
  }

  // ===== REPORT =====
  console.log('=== РЕЗУЛЬТАТ МАТЧИНГА УЛИЦ ===\n');

  const matchedObj = results.reduce((s,r) => s + r.v2.objects, 0);
  const unmatchedObj = unmatched.reduce((s,u) => s + u.objects, 0);
  const skippedObj = skippedNoGeoMapping.reduce((s,u) => s + u.objects, 0);
  const matchedWithObj = results.filter(r => r.v2.objects > 0).length;
  const unmatchedWithObj = unmatched.filter(u => u.objects > 0).length;
  const skippedWithObj = skippedNoGeoMapping.filter(u => u.objects > 0).length;
  const totalWithObj = matchedWithObj + unmatchedWithObj + skippedWithObj;

  console.log(`Всего v2 улиц: ${v2All.length}`);
  console.log(`\nСматчено: ${results.length} улиц (${matchedWithObj} с obj, ${matchedObj} obj)`);
  console.log(`Не сматчено: ${unmatched.length} (${unmatchedWithObj} с obj, ${unmatchedObj} obj)`);
  console.log(`Пропущено (нет geo): ${skippedNoGeoMapping.length} (${skippedWithObj} с obj, ${skippedObj} obj)`);
  console.log(`\nПокрытие (с obj): ${(matchedWithObj/totalWithObj*100).toFixed(1)}%`);
  console.log(`Покрытие (obj count): ${(matchedObj/(matchedObj+unmatchedObj+skippedObj)*100).toFixed(1)}%`);

  // By method
  const byMethod = {};
  for (const r of results) {
    const key = r.method + (r.priority <= 2 ? '_local' : '_cross');
    if (!byMethod[key]) byMethod[key] = {count: 0, objects: 0, withObj: 0};
    byMethod[key].count++;
    byMethod[key].objects += r.v2.objects;
    if (r.v2.objects > 0) byMethod[key].withObj++;
  }
  console.log('\nПо методу:');
  for (const [method, stats] of Object.entries(byMethod).sort((a,b) => b[1].objects - a[1].objects)) {
    console.log(`  ${method}: ${stats.count} улиц (${stats.withObj} с obj, ${stats.objects} obj)`);
  }

  // Unmatched with objects
  const unmatchedWithObjects = unmatched.filter(u => u.objects > 0).sort((a,b) => b.objects - a.objects);
  console.log(`\n--- НЕ СМАТЧЕННЫЕ С ОБЪЕКТАМИ (топ-50) ---`);
  for (const u of unmatchedWithObjects.slice(0, 50)) {
    const ourGeoId = v2GeoToOurGeo[u.geoId];
    const ourGeo = ourGeoId ? ourGeoById[ourGeoId] : null;
    const geoName = ourGeo ? (ourGeo.name_ru || ourGeo.name_uk) : `v2geo:${u.geoId}`;
    console.log(`  v2:${u.id} "${u.nameRu}" / "${u.nameUa}" (${geoName}) — ${u.objects} obj`);
  }
  if (unmatchedWithObjects.length > 50) console.log(`  ... и ещё ${unmatchedWithObjects.length - 50}`);

  // Cross-match examples
  console.log('\n--- КРОСС-МАТЧИ (другой гео, топ-30) ---');
  const crossMatches = results.filter(r => r.priority === 3 && r.v2.objects > 0).sort((a,b) => b.v2.objects - a.v2.objects);
  for (const r of crossMatches.slice(0, 30)) {
    const v2GeoName = ourGeoById[v2GeoToOurGeo[r.v2.geoId]] ? (ourGeoById[v2GeoToOurGeo[r.v2.geoId]].name_ru || ourGeoById[v2GeoToOurGeo[r.v2.geoId]].name_uk) : '?';
    const ourGeo = ourGeoById[regionStreets[getRegionId(v2GeoToOurGeo[r.v2.geoId])]?.find(s => s.id === r.ourId)?.geoId];
    const ourGeoName = ourGeo ? (ourGeo.name_ru || ourGeo.name_uk) : '?';
    console.log(`  v2:${r.v2Id} "${r.v2.nameRu}" (${v2GeoName}) → our:${r.ourId} "${r.ourName}" [${r.method}, conf=${r.confidence.toFixed(2)}, ${r.v2.objects} obj]`);
  }

  // ===== APPLY TO DB =====
  console.log('\n=== ПРИМЕНЕНИЕ К БД ===');

  const deleted = await c.query("DELETE FROM source_id_mappings WHERE source='vector2_crm' AND entity_type='street'");
  console.log(`Удалено старых маппингов: ${deleted.rowCount}`);

  let inserted = 0;
  for (let i = 0; i < results.length; i += 500) {
    const batch = results.slice(i, i + 500);
    const values = batch.map(r =>
      `('vector2_crm', 'street', ${r.v2Id}, ${r.ourId}, ${r.confidence.toFixed(2)}, '${r.method}')`
    ).join(',\n');
    await c.query(`
      INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
      VALUES ${values}
      ON CONFLICT (source, entity_type, source_id) DO UPDATE SET
        local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method
    `);
    inserted += batch.length;
  }
  console.log(`Вставлено: ${inserted}`);

  const finalCount = await c.query("SELECT count(*) FROM source_id_mappings WHERE source='vector2_crm' AND entity_type='street'");
  console.log(`Итого street маппингов: ${finalCount.rows[0].count}`);

  // Save unmatched
  const unmatchedForLookup = unmatchedWithObjects.map(u => {
    const ourGeoId = v2GeoToOurGeo[u.geoId];
    const ourGeo = ourGeoId ? ourGeoById[ourGeoId] : null;
    return {
      v2Id: u.id, nameRu: u.nameRu, nameUa: u.nameUa,
      geoName: ourGeo ? (ourGeo.name_ru || ourGeo.name_uk) : null,
      objects: u.objects
    };
  });
  const outputPath = path.join(__dirname, '..', 'data', 'unmatched_streets.json');
  fs.writeFileSync(outputPath, JSON.stringify(unmatchedForLookup, null, 2));
  console.log(`Сохранено ${unmatchedForLookup.length} несматченных в ${outputPath}`);

  await c.end();
}
main();
