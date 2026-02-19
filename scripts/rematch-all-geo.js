/**
 * Complete re-matching of v2 geo → our geo with region verification.
 *
 * Fixes from v1:
 * - Kiev (27132) self-references parent_id; handle specially
 * - Lviv region (20479) has NULL name_ru; use name_uk
 * - Cross-language normalization: і→е, remove ь for better RU↔UA matching
 * - Some our geo entries have NULL name_ru; use name_uk
 *
 * Does NOT use v2 coordinates (unreliable per user).
 */
const {Client} = require('pg');
const fs = require('fs');
const path = require('path');

// ===== UTILS =====
function parseCSV(filePath) {
  const lines = fs.readFileSync(filePath, 'utf-8').split('\n').filter(l => l.trim());
  const headers = lines[0].split(',');
  return lines.slice(1).filter(l => !l.startsWith('(')).map(l => {
    const vals = l.split(',');
    const row = {};
    headers.forEach((h, i) => row[h] = vals[i] || '');
    return row;
  });
}

// Light normalize: lowercase, strip parentheses, basic letter mappings
function normalize(name) {
  return (name || '')
    .replace(/\s*\(.*?\)\s*/g, ' ')
    .replace(/ё/g, 'е').replace(/Ё/g, 'Е')
    .replace(/ї/g, 'і').replace(/Ї/g, 'І')
    .replace(/є/g, 'е').replace(/Є/g, 'Е')
    .replace(/ґ/g, 'г').replace(/Ґ/g, 'Г')
    .replace(/й/g, 'и').replace(/Й/g, 'И')
    .replace(/ы/g, 'и').replace(/Ы/g, 'И')
    .replace(/['ʼ`'"«»""ʼ']/g, '')
    .replace(/\s+/g, ' ')
    .trim().toLowerCase();
}

// Aggressive cross-language normalize: і→е, remove ь
// Produces same output for Russian and Ukrainian versions of the same word
function normCross(name) {
  return normalize(name)
    .replace(/і/g, 'е')   // Ukrainian і → Russian е
    .replace(/ь/g, '')     // Remove soft sign
    .replace(/\s+/g, ' ').trim();
}

function normStrip(name) {
  return normalize(name)
    .replace(/\s*(район|обл\.?|область|пгт\.?|смт\.?|м\.|г\.|с\.|село|місто|город)\s*/gi, ' ')
    .replace(/\s+/g, ' ').trim();
}

function normStripCross(name) {
  return normCross(name)
    .replace(/\s*(раион|район|обл\.?|област|пгт\.?|смт\.?|м\.|г\.|с\.|село|місто|город|мест|мест)\s*/gi, ' ')
    .replace(/\s+/g, ' ').trim();
}

function extractAllNames(nameRu, nameUa) {
  const names = new Set();

  for (const name of [nameRu, nameUa]) {
    if (!name) continue;
    names.add(normStrip(name));
    names.add(normStripCross(name));
    // Extract from parentheses
    const m = name.match(/\(([^)]+)\)/);
    if (m) {
      names.add(normStrip(m[1]));
      names.add(normStripCross(m[1]));
      names.add(normStrip(name.replace(/\s*\(.*?\)/, '')));
      names.add(normStripCross(name.replace(/\s*\(.*?\)/, '')));
    }
  }

  names.delete('');
  return [...names];
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

  // Load v2 geo
  const v2Geos = parseCSV(path.join(__dirname, '..', 'data', 'vector2_geo.csv'));
  const v2Usage = parseCSV(path.join(__dirname, '..', 'data', 'vector2_geo_usage.csv'));
  const v2UsageMap = {};
  for (const u of v2Usage) v2UsageMap[u.fk_geo_id] = parseInt(u.cnt);

  const v2ById = {};
  for (const g of v2Geos) {
    v2ById[g.geo_id] = {
      id: parseInt(g.geo_id), nameRu: g.geo_name_ru, nameUa: g.geo_name_ua,
      type: parseInt(g.geo_type), lft: parseInt(g.lft), rgt: parseInt(g.rgt),
      objects: v2UsageMap[g.geo_id] || 0
    };
  }

  // Build v2 Nested Set parents
  const v2Sorted = Object.values(v2ById).sort((a,b) => a.lft - b.lft);
  const stack = [];
  for (const g of v2Sorted) {
    while (stack.length > 0 && stack[stack.length - 1].rgt < g.lft) stack.pop();
    g.parentCity = null; g.parentRegion = null;
    for (const anc of stack) {
      if (anc.type === 3) g.parentCity = anc;
      if (anc.type === 2) g.parentRegion = anc;
    }
    stack.push(g);
  }

  // Load our geo
  const ourGeos = await c.query("SELECT id, name->>'ru' as name_ru, name->>'uk' as name_uk, type, parent_id FROM geo");
  const ourById = {};
  for (const g of ourGeos.rows) ourById[g.id] = g;

  // FIX: Kiev (27132) self-references. Find Киевская область region.
  const KYIV_CITY_ID = 27132;
  const KYIV_REGION_ID = 19338; // Киевская область

  // Build our region lookup with Kiev fix
  function getOurRegionId(geoId) {
    const visited = new Set();
    let cur = ourById[geoId];
    for (let i = 0; i < 15 && cur; i++) {
      if (visited.has(cur.id)) break; // circular reference
      visited.add(cur.id);
      if (cur.type === 'region') return cur.id;
      // Special: Kiev city → Киевская область
      if (cur.id === KYIV_CITY_ID) return KYIV_REGION_ID;
      cur = cur.parent_id ? ourById[cur.parent_id] : null;
    }
    return null;
  }

  const ourRegions = ourGeos.rows.filter(g => g.type === 'region');
  console.log('Our regions:');
  for (const r of ourRegions) {
    console.log(`  id:${r.id} ru:"${r.name_ru}" uk:"${r.name_uk}"`);
  }

  // Map v2 region → our region (using BOTH ru and uk names)
  const v2RegionToOurRegion = {};
  for (const v2 of Object.values(v2ById)) {
    if (v2.type !== 2) continue;
    const v2normRu = normStripCross(v2.nameRu);
    const v2normUa = normStripCross(v2.nameUa);
    for (const our of ourRegions) {
      const ourNormRu = our.name_ru ? normStripCross(our.name_ru) : '';
      const ourNormUk = our.name_uk ? normStripCross(our.name_uk) : '';
      // Try all combinations
      if ((v2normRu && ourNormRu && (v2normRu === ourNormRu || v2normRu.substring(0, 6) === ourNormRu.substring(0, 6))) ||
          (v2normUa && ourNormUk && (v2normUa === ourNormUk || v2normUa.substring(0, 6) === ourNormUk.substring(0, 6))) ||
          (v2normRu && ourNormUk && (v2normRu === ourNormUk || v2normRu.substring(0, 6) === ourNormUk.substring(0, 6))) ||
          (v2normUa && ourNormRu && (v2normUa === ourNormRu || v2normUa.substring(0, 6) === ourNormRu.substring(0, 6)))) {
        v2RegionToOurRegion[v2.id] = our.id;
        break;
      }
    }
  }
  console.log(`\nMapped v2 regions → our regions: ${Object.keys(v2RegionToOurRegion).length}`);
  for (const [v2id, ourId] of Object.entries(v2RegionToOurRegion)) {
    const v2 = v2ById[v2id];
    const our = ourById[ourId];
    console.log(`  v2:${v2id} "${v2.nameRu}" → our:${ourId} "${our.name_ru || our.name_uk}"`);
  }

  // Build index: our geos by region
  const ourByRegion = {};
  for (const g of ourGeos.rows) {
    const regionId = getOurRegionId(g.id);
    if (!regionId) continue;
    if (!ourByRegion[regionId]) ourByRegion[regionId] = [];
    const normNames = extractAllNames(g.name_ru, g.name_uk);
    ourByRegion[regionId].push({...g, normNames});
  }

  // Stats
  for (const [regId, geos] of Object.entries(ourByRegion)) {
    const reg = ourById[regId];
    console.log(`  Region ${regId} "${reg ? (reg.name_ru || reg.name_uk) : '?'}": ${geos.length} geos`);
  }

  // v2 type → our type mapping
  const typeMap = {1: null, 2: 'region', 3: 'city', 4: 'city_district', 5: 'region_district', 6: 'village'};

  // ===== MATCHING =====
  const results = [];
  const unmatched = [];
  const skippedRegion = [];
  const skippedType = [];

  const v2WithObjects = Object.values(v2ById).filter(g => g.objects > 0).sort((a,b) => b.objects - a.objects);
  console.log(`\nMatching ${v2WithObjects.length} v2 geos with objects...`);

  for (const v2 of v2WithObjects) {
    if (v2.type <= 2) { skippedType.push(v2); continue; }

    const v2RegionId = v2.parentRegion ? v2.parentRegion.id : null;
    const ourRegionId = v2RegionId ? v2RegionToOurRegion[v2RegionId] : null;

    if (!ourRegionId) { skippedRegion.push(v2); continue; }

    const candidates = ourByRegion[ourRegionId] || [];
    const v2Names = extractAllNames(v2.nameRu, v2.nameUa);
    const v2Type = typeMap[v2.type];

    let bestMatch = null;
    let bestMethod = null;
    let bestConf = 0;

    // Method 1: Exact name + same type (including cross-language norm)
    for (const name of v2Names) {
      for (const cand of candidates) {
        if (cand.type === v2Type && cand.normNames.includes(name)) {
          bestMatch = cand; bestMethod = 'exact_name_type'; bestConf = 1.0;
          break;
        }
      }
      if (bestConf >= 1.0) break;
    }

    // Method 2: Exact name, any type
    if (!bestMatch) {
      for (const name of v2Names) {
        for (const cand of candidates) {
          if (cand.normNames.includes(name)) {
            bestMatch = cand; bestMethod = 'exact_name_anytype'; bestConf = 0.95;
            break;
          }
        }
        if (bestMatch) break;
      }
    }

    // Method 3: Substring containment
    if (!bestMatch) {
      let bestSubSim = 0;
      for (const name of v2Names) {
        if (name.length < 4) continue;
        for (const cand of candidates) {
          for (const cn of cand.normNames) {
            if (cn.length < 4) continue;
            if (name.includes(cn) || cn.includes(name)) {
              const sim = similarity(name, cn);
              if (sim >= 0.75 && sim > bestSubSim) {
                bestMatch = cand; bestMethod = 'substring_match'; bestConf = sim; bestSubSim = sim;
              }
            }
          }
        }
      }
    }

    // Method 4: Fuzzy match (Levenshtein ≥ 0.80)
    if (!bestMatch) {
      let bestSim = 0;
      for (const name of v2Names) {
        if (name.length < 3) continue;
        for (const cand of candidates) {
          if (v2Type === 'city_district' && cand.type !== 'city_district' && cand.type !== 'city') continue;
          for (const cn of cand.normNames) {
            if (cn.length < 3) continue;
            const sim = similarity(name, cn);
            if (sim >= 0.80 && sim > bestSim) {
              bestMatch = cand; bestMethod = 'fuzzy_match'; bestConf = sim; bestSim = sim;
            }
          }
        }
      }
    }

    // Method 5: For city_districts — fall back to parent city
    if (!bestMatch && v2Type === 'city_district') {
      const v2CityName = v2.parentCity ? normStripCross(v2.parentCity.nameRu) : null;
      const v2CityNameUa = v2.parentCity ? normStripCross(v2.parentCity.nameUa) : null;
      if (v2CityName || v2CityNameUa) {
        for (const cand of candidates) {
          if (cand.type !== 'city') continue;
          for (const cn of cand.normNames) {
            if (cn === v2CityName || cn === v2CityNameUa) {
              bestMatch = cand; bestMethod = 'city_fallback'; bestConf = 0.80;
              break;
            }
          }
          if (bestMatch) break;
        }
      }
    }

    if (bestMatch) {
      results.push({v2Id: v2.id, ourId: bestMatch.id, method: bestMethod, confidence: bestConf, v2,
        ourName: bestMatch.name_ru || bestMatch.name_uk, ourType: bestMatch.type});
    } else {
      unmatched.push(v2);
    }
  }

  // ===== REPORT =====
  console.log('\n=== РЕЗУЛЬТАТ МАТЧИНГА ===');
  const matchedObj = results.reduce((s,r) => s+r.v2.objects, 0);
  const unmatchedObj = unmatched.reduce((s,u) => s+u.objects, 0);
  const skippedObj = skippedRegion.reduce((s,u) => s+u.objects, 0);
  const totalObj = matchedObj + unmatchedObj + skippedObj;
  console.log(`Сматчено: ${results.length} гео (${matchedObj} obj, ${(matchedObj/(matchedObj+unmatchedObj)*100).toFixed(1)}% из наших регионов)`);
  console.log(`Не сматчено: ${unmatched.length} гео (${unmatchedObj} obj)`);
  console.log(`Пропущено (регион не в БД): ${skippedRegion.length} (${skippedObj} obj)`);
  console.log(`Пропущено (страна/регион): ${skippedType.length}`);

  // By method
  const byMethod = {};
  for (const r of results) {
    if (!byMethod[r.method]) byMethod[r.method] = {count: 0, objects: 0};
    byMethod[r.method].count++;
    byMethod[r.method].objects += r.v2.objects;
  }
  console.log('\nПо методу:');
  for (const [method, stats] of Object.entries(byMethod).sort((a,b) => b[1].objects - a[1].objects)) {
    console.log(`  ${method}: ${stats.count} гео, ${stats.objects} obj`);
  }

  // Show some matches for verification
  console.log('\n--- ПРИМЕРЫ МАТЧЕЙ (fuzzy) ---');
  for (const r of results.filter(r => r.method === 'fuzzy_match').slice(0, 20)) {
    console.log(`  v2:${r.v2Id} "${r.v2.nameRu}" → our:${r.ourId} "${r.ourName}" (${r.ourType}) [conf=${r.confidence.toFixed(2)}]`);
  }

  console.log('\n--- ПРИМЕРЫ МАТЧЕЙ (city_fallback) ---');
  for (const r of results.filter(r => r.method === 'city_fallback').slice(0, 20)) {
    console.log(`  v2:${r.v2Id} "${r.v2.nameRu}" (${r.v2.parentCity ? r.v2.parentCity.nameRu : '?'}) → our:${r.ourId} "${r.ourName}" (${r.ourType}) [conf=${r.confidence.toFixed(2)}]`);
  }

  // Unmatched details
  console.log('\n--- НЕ СМАТЧЕННЫЕ (все) ---');
  unmatched.sort((a,b) => b.objects - a.objects);
  for (const u of unmatched.slice(0, 50)) {
    const city = u.parentCity ? u.parentCity.nameRu : '';
    const region = u.parentRegion ? u.parentRegion.nameRu : '?';
    console.log(`  v2:${u.id} "${u.nameRu}" / "${u.nameUa}" type:${u.type} (${city}, ${region}) — ${u.objects} obj`);
  }
  if (unmatched.length > 50) console.log(`  ... и ещё ${unmatched.length - 50}`);

  // Skipped region details
  console.log('\n--- ПРОПУЩЕНО (регион не в БД, топ-20) ---');
  skippedRegion.sort((a,b) => b.objects - a.objects);
  for (const u of skippedRegion.slice(0, 20)) {
    const region = u.parentRegion ? u.parentRegion.nameRu : '?';
    console.log(`  v2:${u.id} "${u.nameRu}" (${region}) — ${u.objects} obj`);
  }

  // ===== APPLY TO DB =====
  console.log('\n=== ПРИМЕНЕНИЕ К БД ===');

  const deleted = await c.query("DELETE FROM source_id_mappings WHERE source='vector2_crm' AND entity_type='geo'");
  console.log(`Удалено старых маппингов: ${deleted.rowCount}`);

  let inserted = 0;
  for (let i = 0; i < results.length; i += 500) {
    const batch = results.slice(i, i + 500);
    const values = batch.map(r =>
      `('vector2_crm', 'geo', ${r.v2Id}, ${r.ourId}, ${r.confidence.toFixed(2)}, '${r.method}')`
    ).join(',\n');
    await c.query(`
      INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
      VALUES ${values}
      ON CONFLICT (source, entity_type, source_id) DO UPDATE SET
        local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method
    `);
    inserted += batch.length;
  }
  console.log(`Вставлено новых маппингов: ${inserted}`);

  const finalCount = await c.query("SELECT count(*) FROM source_id_mappings WHERE source='vector2_crm' AND entity_type='geo'");
  console.log(`Итого geo маппингов: ${finalCount.rows[0].count}`);

  // Dupe check
  const dupes = await c.query(`
    SELECT local_id, count(*) as cnt FROM source_id_mappings
    WHERE source='vector2_crm' AND entity_type='geo'
    GROUP BY local_id HAVING count(*) > 1
    ORDER BY count(*) DESC LIMIT 10
  `);
  console.log(`\nДублей (несколько v2 → один наш): ${dupes.rows.length}`);
  for (const d of dupes.rows.slice(0, 5)) {
    const g = ourById[d.local_id];
    console.log(`  our:${d.local_id} "${g ? (g.name_ru || g.name_uk) : '?'}" ← ${d.cnt} v2 IDs`);
  }

  await c.end();
}
main();
