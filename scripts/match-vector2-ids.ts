/**
 * One-time script to match vector2 (old CRM) geo/street/complex IDs
 * to our local database IDs and populate source_id_mappings table.
 *
 * Usage: npx ts-node scripts/match-vector2-ids.ts
 *
 * Prerequisites:
 *   CSV files in $TEMP/:
 *     - vector2_geo.csv (from vec.atlanta.ua vector2.geo)
 *     - vector2_voc04.csv (from vec.atlanta.ua vector2.voc04 — streets)
 *     - vector2_voc33.csv (from vec.atlanta.ua vector2.voc33 — complexes)
 */

import * as fs from 'fs';
import { Client } from 'pg';

const SOURCE = 'vector2_crm';

// ============================================================
// CSV parsing
// ============================================================

function parseCSV(filePath: string): Record<string, string>[] {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter((l) => l.trim());
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const rows: Record<string, string>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] || '';
    }
    rows.push(row);
  }
  return rows;
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

// ============================================================
// Normalization
// ============================================================

function normalize(s: string | undefined | null): string {
  if (!s) return '';
  return s
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/ї/g, 'i')
    .replace(/є/g, 'е')
    .replace(/і/g, 'и')
    .replace(/ґ/g, 'г')
    .replace(/[«»"'`'ʼ]/g, '')
    .replace(/жк\s+/g, '')
    .replace(/кг\s+/g, '')
    .replace(/ск\s+/g, '')
    .replace(/ст\s+/g, '')
    .replace(/[^а-яa-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeStreet(s: string | undefined | null): string {
  if (!s) return '';
  let r = s.toLowerCase();
  // Transliterate
  r = r.replace(/ё/g, 'е').replace(/ї/g, 'i').replace(/є/g, 'е').replace(/і/g, 'и').replace(/ґ/g, 'г');
  // Strip parenthetical alternatives: "(10-я линия)" → ""
  r = r.replace(/\(.*?\)/g, '');
  // Strip ordinal suffixes: "10-го" → "10", "1-я" → "1", "27-ма" → "27"
  r = r.replace(/(\d+)-(?:го|й|я|ой|ей|ая|ий|ої|а|ій|ма|та|ша|га|ва|тя|ге)/g, '$1');
  // Strip quotes and special chars early
  r = r.replace(/[«»"'`'ʼ.,:;]/g, ' ');
  // Strip full street type words (anywhere in the string)
  const typeWords = [
    'вулиця', 'улица', 'провулок', 'переулок', 'проспект',
    'бульвар', 'дорога', 'шосе', 'шоссе', 'набережна', 'набережная',
    'площадь', 'площа', 'узвіз', 'спуск', 'тупик', 'сквер',
    'линия', 'линія', 'алея', 'аллея', 'майдан', 'узвиз',
  ];
  for (const tw of typeWords) {
    r = r.split(tw).join(' ');
  }
  // Strip abbreviated types (after dots are already replaced with spaces)
  const abbrWords = [
    'вул', 'ул', 'пров', 'пер', 'просп', 'пр-т',
    'бульв', 'б-р', 'дор', 'пл', 'туп', 'скв',
  ];
  for (const aw of abbrWords) {
    // Only strip if surrounded by space/start/end (poor man's word boundary for cyrillic)
    r = r.replace(new RegExp(`(^|\\s)${aw.replace('-', '\\-')}(\\s|$)`, 'g'), ' ');
  }
  // Remove non-alphanumeric (keep cyrillic, latin, digits, spaces, hyphens)
  r = r.replace(/[^а-яa-z0-9\s-]/g, '');
  r = r.replace(/\s+/g, ' ').trim();
  return r;
}

function parseJsonbName(jsonStr: string): { ru: string; uk: string; en: string } {
  try {
    const obj = JSON.parse(jsonStr);
    return {
      ru: obj.ru || '',
      uk: obj.uk || '',
      en: obj.en || '',
    };
  } catch {
    return { ru: '', uk: '', en: '' };
  }
}

// ============================================================
// Geo type mapping
// ============================================================

const VECTOR2_GEO_TYPE_MAP: Record<number, string> = {
  1: 'country',
  2: 'region',
  3: 'city',
  4: 'city_district',
  5: 'region_district',
  6: 'village',
};

// ============================================================
// Main
// ============================================================

async function main() {
  console.log('=== Vector2 ID Matching Script ===\n');

  // Connect to local DB
  const client = new Client({
    host: 'localhost',
    port: 5433,
    user: 'postgres',
    password: 'postgres',
    database: 'valuation',
  });
  await client.connect();
  console.log('Connected to local DB\n');

  // Clear previous mappings for this source
  await client.query(`DELETE FROM source_id_mappings WHERE source = $1`, [SOURCE]);
  console.log('Cleared previous mappings\n');

  // Load CSV data
  console.log('Loading CSV files...');
  const tmpDir = process.env.TEMP || process.env.TMP || '/tmp';
  const vector2Geo = parseCSV(`${tmpDir}/vector2_geo.csv`);
  const vector2Voc04 = parseCSV(`${tmpDir}/vector2_voc04.csv`);
  const vector2Voc33 = parseCSV(`${tmpDir}/vector2_voc33.csv`);

  console.log(`  vector2_geo: ${vector2Geo.length} rows`);
  console.log(`  vector2_voc04 (streets): ${vector2Voc04.length} rows`);
  console.log(`  vector2_voc33 (complexes): ${vector2Voc33.length} rows\n`);

  // ========================================
  // Phase 1: Match GEO
  // ========================================
  console.log('=== Phase 1: Matching GEO ===');

  // Load our geo from DB
  const ourGeoResult = await client.query(
    `SELECT id, name->>'ru' as name_ru, name->>'uk' as name_uk, type, parent_id FROM geo`,
  );
  const ourGeo = ourGeoResult.rows;
  console.log(`Our geo: ${ourGeo.length} entries`);

  // Build lookup maps for our geo: normalized_name+type → [entries]
  const ourGeoByNameType = new Map<string, typeof ourGeo>();
  for (const g of ourGeo) {
    const keyRu = `${normalize(g.name_ru)}|${g.type}`;
    const keyUk = `${normalize(g.name_uk)}|${g.type}`;
    for (const key of [keyRu, keyUk]) {
      if (!key.startsWith('|')) {
        const arr = ourGeoByNameType.get(key) || [];
        arr.push(g);
        ourGeoByNameType.set(key, arr);
      }
    }
  }

  // Also build aggregator geo_id → vector2 geo info (since they share IDs)
  // Actually vector2 geo IS the source, aggregator just copies. So vector2_geo_id = aggregator_geo_id.
  // We match vector2 geo directly to our geo.

  let geoMatched = 0;
  let geoUnmatched = 0;
  const geoMapping = new Map<number, number>(); // vector2_geo_id → our_geo_id

  const geoInserts: string[] = [];

  for (const v2g of vector2Geo) {
    const v2Id = parseInt(v2g.geo_id);
    const v2Type = VECTOR2_GEO_TYPE_MAP[parseInt(v2g.geo_type)];
    if (!v2Type) continue;

    const nameRu = v2g.geo_name_ru;
    const nameUa = v2g.geo_name_ua;

    // Try exact match by normalized name + type
    let candidates =
      ourGeoByNameType.get(`${normalize(nameRu)}|${v2Type}`) ||
      ourGeoByNameType.get(`${normalize(nameUa)}|${v2Type}`) ||
      [];

    let matchMethod = 'exact_name';
    let confidence = 1.0;

    if (candidates.length === 0) {
      // Try without type (maybe type mismatch)
      for (const [key, entries] of ourGeoByNameType) {
        if (key.startsWith(`${normalize(nameRu)}|`) || key.startsWith(`${normalize(nameUa)}|`)) {
          candidates = entries;
          matchMethod = 'name_type_mismatch';
          confidence = 0.8;
          break;
        }
      }
    }

    // For city_districts: try stripping "район" or adding it
    if (candidates.length === 0 && v2Type === 'city_district') {
      const nameVariants = [nameRu, nameUa].filter(Boolean);
      for (const name of nameVariants) {
        // Add "район" if missing
        const withRayon = normalize(name + ' район');
        // Remove "район" if present
        const withoutRayon = normalize(name.replace(/\s*район\s*/g, '').replace(/\(.*?\)/g, ''));
        for (const variant of [withRayon, withoutRayon]) {
          candidates = ourGeoByNameType.get(`${variant}|city_district`) || [];
          if (candidates.length > 0) {
            matchMethod = 'district_rayon_match';
            confidence = 0.9;
            break;
          }
        }
        if (candidates.length > 0) break;
        // Try partial: our name contains vector2 name
        for (const [key, entries] of ourGeoByNameType) {
          if (!key.endsWith('|city_district')) continue;
          const ourName = key.split('|')[0];
          if (ourName.includes(normalize(name)) || normalize(name).includes(ourName)) {
            candidates = entries;
            matchMethod = 'district_partial';
            confidence = 0.75;
            break;
          }
        }
        if (candidates.length > 0) break;
      }
    }

    if (candidates.length === 1) {
      geoMapping.set(v2Id, candidates[0].id);
      geoInserts.push(
        `('${SOURCE}', 'geo', ${v2Id}, ${candidates[0].id}, ${confidence}, '${matchMethod}')`,
      );
      geoMatched++;
    } else if (candidates.length > 1) {
      // Multiple matches — pick first (could be improved with parent matching)
      geoMapping.set(v2Id, candidates[0].id);
      geoInserts.push(
        `('${SOURCE}', 'geo', ${v2Id}, ${candidates[0].id}, 0.7, 'ambiguous_name')`,
      );
      geoMatched++;
    } else {
      geoUnmatched++;
      if (parseInt(v2g.geo_type) <= 4) {
        // Only log cities and above, not villages
        console.log(`  UNMATCHED geo: ${v2Id} "${nameRu}" (${v2Type})`);
      }
    }
  }

  // Batch insert geo mappings
  if (geoInserts.length > 0) {
    const batchSize = 500;
    for (let i = 0; i < geoInserts.length; i += batchSize) {
      const batch = geoInserts.slice(i, i + batchSize);
      await client.query(
        `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
         VALUES ${batch.join(',')}
         ON CONFLICT (source, entity_type, source_id) DO UPDATE
         SET local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method`,
      );
    }
  }

  console.log(`Geo: matched=${geoMatched}, unmatched=${geoUnmatched}\n`);

  // ========================================
  // Phase 2: Match STREETS (from vector2.voc04)
  // ========================================
  console.log('=== Phase 2: Matching STREETS ===');

  // Load our streets from DB
  const ourStreetsResult = await client.query(
    `SELECT id, name->>'ru' as name_ru, name->>'uk' as name_uk, geo_id FROM streets`,
  );
  const ourStreets = ourStreetsResult.rows;
  console.log(`Our streets: ${ourStreets.length} entries`);
  console.log(`Vector2 streets (voc04): ${vector2Voc04.length} entries`);

  // Build parent geo lookup: our city_district geo_id → parent city geo_id
  const ourGeoParentResult = await client.query(
    `SELECT id, parent_id, type FROM geo WHERE type = 'city_district'`,
  );
  const districtToCity = new Map<number, number>();
  for (const g of ourGeoParentResult.rows) {
    if (g.parent_id) districtToCity.set(g.id, g.parent_id);
  }
  console.log(`City districts with parents: ${districtToCity.size}`);

  // Build vector2 geo parent mapping using nested set (lft/rgt) for city-level fallback
  // For city_districts (type=4), find parent city (type=3) via nested set
  const v2GeoParent = new Map<number, number>(); // v2_geo_id → v2_parent_city_geo_id
  const v2Cities = vector2Geo.filter((g) => parseInt(g.geo_type) === 3);
  for (const v2g of vector2Geo) {
    if (parseInt(v2g.geo_type) !== 4) continue; // only city_districts
    const id = parseInt(v2g.geo_id);
    const lft = parseInt(v2g.lft);
    const rgt = parseInt(v2g.rgt);
    // Find parent city: city whose lft < district.lft AND rgt > district.rgt (smallest range)
    let bestCity: any = null;
    let bestRange = Infinity;
    for (const city of v2Cities) {
      const cLft = parseInt(city.lft);
      const cRgt = parseInt(city.rgt);
      if (cLft < lft && cRgt > rgt) {
        const range = cRgt - cLft;
        if (range < bestRange) {
          bestRange = range;
          bestCity = city;
        }
      }
    }
    if (bestCity) {
      v2GeoParent.set(id, parseInt(bestCity.geo_id));
    }
  }
  console.log(`Vector2 district→city mappings: ${v2GeoParent.size}`);

  // Build lookup: normalized_name + our_geo_id → [street entries]
  const ourStreetsByNameGeo = new Map<string, typeof ourStreets>();
  for (const s of ourStreets) {
    for (const name of [s.name_ru, s.name_uk]) {
      const norm = normalizeStreet(name);
      if (!norm) continue;
      const key = `${norm}|${s.geo_id}`;
      const arr = ourStreetsByNameGeo.get(key) || [];
      arr.push(s);
      ourStreetsByNameGeo.set(key, arr);
    }
  }

  // Build name-only lookup for fallback
  const ourStreetsByName = new Map<string, typeof ourStreets>();
  for (const s of ourStreets) {
    for (const name of [s.name_ru, s.name_uk]) {
      const norm = normalizeStreet(name);
      if (!norm) continue;
      const arr = ourStreetsByName.get(norm) || [];
      arr.push(s);
      ourStreetsByName.set(norm, arr);
    }
  }

  let streetMatched = 0;
  let streetUnmatched = 0;
  let streetNoGeo = 0;
  const streetMethodStats = new Map<string, number>();
  const streetInserts: string[] = [];

  for (const v2s of vector2Voc04) {
    const v2Kod = parseInt(v2s.kod);
    const v2GeoId = parseInt(v2s.fk_geoid);
    const nameRu = v2s.name_ru;
    const nameUa = v2s.name_ua;

    if (!v2Kod || isNaN(v2Kod)) continue;

    // Resolve vector2 geo_id → our geo_id
    let ourGeoId = geoMapping.get(v2GeoId);

    // City-level fallback: if vector2 geo is a district, try parent city
    let ourCityGeoId: number | undefined;
    if (ourGeoId) {
      // Check if our mapped geo is a city_district → get parent city
      ourCityGeoId = districtToCity.get(ourGeoId);
    }
    if (!ourGeoId) {
      // If direct geo not mapped, try vector2's parent geo (for city_districts)
      const v2ParentGeoId = v2GeoParent.get(v2GeoId);
      if (v2ParentGeoId) {
        ourGeoId = geoMapping.get(v2ParentGeoId);
        if (ourGeoId) ourCityGeoId = ourGeoId; // parent is already the city
      }
    }

    const normRu = normalizeStreet(nameRu);
    const normUa = normalizeStreet(nameUa);

    let candidates: typeof ourStreets = [];
    let matchMethod = 'exact_name_geo';
    let confidence = 1.0;

    if (ourGeoId) {
      // Strategy 1: normalized name + exact geo
      candidates =
        ourStreetsByNameGeo.get(`${normRu}|${ourGeoId}`) ||
        ourStreetsByNameGeo.get(`${normUa}|${ourGeoId}`) ||
        [];

      // Strategy 2: normalized name + parent city geo (if geo was district)
      if (candidates.length === 0 && ourCityGeoId && ourCityGeoId !== ourGeoId) {
        candidates =
          ourStreetsByNameGeo.get(`${normRu}|${ourCityGeoId}`) ||
          ourStreetsByNameGeo.get(`${normUa}|${ourCityGeoId}`) ||
          [];
        if (candidates.length > 0) {
          matchMethod = 'name_parent_city';
          confidence = 0.9;
        }
      }

      // Strategy 3: name-only filtered by geo or parent city
      if (candidates.length === 0) {
        const byNameRu = ourStreetsByName.get(normRu) || [];
        const byNameUa = ourStreetsByName.get(normUa) || [];
        const combined = byNameRu.length > 0 ? byNameRu : byNameUa;

        const geoIds = [ourGeoId];
        if (ourCityGeoId) geoIds.push(ourCityGeoId);
        const geoFiltered = combined.filter((s: any) => geoIds.includes(s.geo_id));

        if (geoFiltered.length > 0) {
          candidates = geoFiltered;
          matchMethod = 'name_filtered_geo';
          confidence = 0.8;
        }
      }

      // Strategy 4: try alias from voc04
      if (candidates.length === 0 && v2s.alias) {
        const normAlias = normalizeStreet(v2s.alias);
        if (normAlias) {
          const geoIds = [ourGeoId];
          if (ourCityGeoId) geoIds.push(ourCityGeoId);
          for (const gid of geoIds) {
            candidates = ourStreetsByNameGeo.get(`${normAlias}|${gid}`) || [];
            if (candidates.length > 0) {
              matchMethod = 'alias_geo';
              confidence = 0.85;
              break;
            }
          }
        }
      }
    } else {
      streetNoGeo++;
      // Fallback: name-only match (only if unique)
      const byNameRu = ourStreetsByName.get(normRu) || [];
      const byNameUa = ourStreetsByName.get(normUa) || [];
      const combined = byNameRu.length > 0 ? byNameRu : byNameUa;
      if (combined.length === 1) {
        candidates = combined;
        matchMethod = 'name_only_no_geo';
        confidence = 0.5;
      }
    }

    if (candidates.length >= 1) {
      const best = candidates[0];
      const conf = candidates.length === 1 ? confidence : Math.min(confidence, 0.6);
      const method = candidates.length === 1 ? matchMethod : 'ambiguous';
      streetInserts.push(
        `('${SOURCE}', 'street', ${v2Kod}, ${best.id}, ${conf}, '${method}')`,
      );
      streetMatched++;
      streetMethodStats.set(method, (streetMethodStats.get(method) || 0) + 1);
    } else {
      streetUnmatched++;
    }
  }

  // Batch insert street mappings
  if (streetInserts.length > 0) {
    const batchSize = 500;
    for (let i = 0; i < streetInserts.length; i += batchSize) {
      const batch = streetInserts.slice(i, i + batchSize);
      await client.query(
        `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
         VALUES ${batch.join(',')}
         ON CONFLICT (source, entity_type, source_id) DO UPDATE
         SET local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method`,
      );
    }
  }

  console.log(
    `Streets (phase 2): matched=${streetMatched}, unmatched=${streetUnmatched}, noGeo=${streetNoGeo}`,
  );
  console.log('  Match methods:');
  for (const [method, count] of [...streetMethodStats.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${method}: ${count}`);
  }
  console.log();

  // ========================================
  // Phase 2.5: OSM old_name matching for unmatched streets
  // ========================================
  console.log('=== Phase 2.5: OSM old_name matching ===');

  const osmOldToCurrentPath = `${tmpDir}/osm_old_to_current.csv`;
  const osmAllStreetsPath = `${tmpDir}/osm_all_streets.csv`;

  const matchedStreetSet = new Set(
    streetInserts.map((ins) => {
      // Extract source_id from insert string: ('vector2_crm', 'street', <source_id>, ...)
      const m = ins.match(/'street',\s*(\d+)/);
      return m ? parseInt(m[1]) : 0;
    }),
  );

  if (fs.existsSync(osmOldToCurrentPath) && fs.existsSync(osmAllStreetsPath)) {
    // Load old→current mapping
    const osmOldToCurrent = parseCSV(osmOldToCurrentPath);
    console.log(`OSM old→current mappings: ${osmOldToCurrent.length}`);

    // Build normalized old_name → { currentNameRu, currentNameUk, currentName }
    const oldNameMap = new Map<string, { currentName: string; currentNameRu: string; currentNameUk: string }>();
    for (const row of osmOldToCurrent) {
      const oldNorm = normalizeStreet(row.old_name);
      if (oldNorm) {
        oldNameMap.set(oldNorm, {
          currentName: row.current_name || '',
          currentNameRu: row.current_name_ru || '',
          currentNameUk: row.current_name_uk || '',
        });
      }
    }

    // Load ALL OSM streets for name:ru matching
    const osmAllStreets = parseCSV(osmAllStreetsPath);
    console.log(`OSM all streets: ${osmAllStreets.length}`);

    // Build OSM name_ru → name_uk/name map (for format bridging)
    const osmNameRuToStreet = new Map<string, { name: string; nameRu: string; nameUk: string }>();
    for (const row of osmAllStreets) {
      const nameRu = row.name_ru;
      const nameUk = row.name_uk;
      if (nameRu) {
        const norm = normalizeStreet(nameRu);
        if (norm) osmNameRuToStreet.set(norm, { name: row.name || '', nameRu, nameUk: nameUk || '' });
      }
      if (nameUk) {
        const norm = normalizeStreet(nameUk);
        if (norm) osmNameRuToStreet.set(norm, { name: row.name || '', nameRu: nameRu || '', nameUk });
      }
      // Also parse old_names (pipe-separated)
      if (row.old_names) {
        for (const oldN of row.old_names.split('|')) {
          const on = oldN.trim();
          if (on) {
            const norm = normalizeStreet(on);
            if (norm) {
              oldNameMap.set(norm, {
                currentName: row.name || '',
                currentNameRu: nameRu || '',
                currentNameUk: nameUk || '',
              });
            }
          }
        }
      }
    }
    console.log(`Old name lookup entries: ${oldNameMap.size}`);

    // Odessa geo IDs (vector2 geo 2 → our geo 18271, and related districts)
    const odessaOurGeoId = geoMapping.get(2); // vector2 Одесса → our Одесса
    const odessaGeoIds = new Set<number>();
    if (odessaOurGeoId) {
      odessaGeoIds.add(odessaOurGeoId);
      // Also add Odessa districts
      for (const [distId, cityId] of districtToCity) {
        if (cityId === odessaOurGeoId) odessaGeoIds.add(distId);
      }
    }

    let osmMatched = 0;
    let osmChecked = 0;
    const osmInserts: string[] = [];

    for (const v2s of vector2Voc04) {
      const v2Kod = parseInt(v2s.kod);
      if (!v2Kod || isNaN(v2Kod) || matchedStreetSet.has(v2Kod)) continue;

      const v2GeoId = parseInt(v2s.fk_geoid);
      // Only process streets with geo mapping (focus on cities we have)
      let ourGeoId = geoMapping.get(v2GeoId);
      if (!ourGeoId) {
        const v2Parent = v2GeoParent.get(v2GeoId);
        if (v2Parent) ourGeoId = geoMapping.get(v2Parent);
      }
      if (!ourGeoId) continue;

      osmChecked++;
      const nameRu = v2s.name_ru;
      const nameUa = v2s.name_ua;
      const normRu = normalizeStreet(nameRu);
      const normUa = normalizeStreet(nameUa);

      // Strategy 1: Check if vector2 name is an old_name in OSM
      let currentInfo = oldNameMap.get(normRu) || oldNameMap.get(normUa);
      let matchMethod = 'osm_old_name';

      // Strategy 2: If not found as old_name, try finding via OSM as name bridge
      // (vector2 "Балковская" might be in OSM as name:ru, giving us the uk name to match)
      if (!currentInfo) {
        const osmEntry = osmNameRuToStreet.get(normRu) || osmNameRuToStreet.get(normUa);
        if (osmEntry) {
          currentInfo = {
            currentName: osmEntry.name,
            currentNameRu: osmEntry.nameRu,
            currentNameUk: osmEntry.nameUk,
          };
          matchMethod = 'osm_name_bridge';
        }
      }

      if (!currentInfo) continue;

      // Now try to find the current name in our DB
      const currentNormRu = normalizeStreet(currentInfo.currentNameRu);
      const currentNormUk = normalizeStreet(currentInfo.currentNameUk);
      const currentNormName = normalizeStreet(currentInfo.currentName);

      // Collect geo IDs to search: mapped geo + parent city
      const searchGeoIds: number[] = [ourGeoId];
      const parentCity = districtToCity.get(ourGeoId);
      if (parentCity) searchGeoIds.push(parentCity);

      let candidates: typeof ourStreets = [];
      for (const norm of [currentNormRu, currentNormUk, currentNormName]) {
        if (!norm) continue;
        for (const gid of searchGeoIds) {
          candidates = ourStreetsByNameGeo.get(`${norm}|${gid}`) || [];
          if (candidates.length > 0) break;
        }
        if (candidates.length > 0) break;
        // Also try name-only fallback filtered by geo
        const byName = ourStreetsByName.get(norm) || [];
        candidates = byName.filter((s: any) => searchGeoIds.includes(s.geo_id));
        if (candidates.length > 0) break;
      }

      if (candidates.length >= 1) {
        const best = candidates[0];
        const conf = candidates.length === 1 ? 0.85 : 0.6;
        const method = candidates.length === 1 ? matchMethod : `${matchMethod}_ambiguous`;
        osmInserts.push(
          `('${SOURCE}', 'street', ${v2Kod}, ${best.id}, ${conf}, '${method}')`,
        );
        osmMatched++;
        streetMethodStats.set(method, (streetMethodStats.get(method) || 0) + 1);
      }
    }

    // Batch insert OSM-matched streets
    if (osmInserts.length > 0) {
      const batchSize = 500;
      for (let i = 0; i < osmInserts.length; i += batchSize) {
        const batch = osmInserts.slice(i, i + batchSize);
        await client.query(
          `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
           VALUES ${batch.join(',')}
           ON CONFLICT (source, entity_type, source_id) DO UPDATE
           SET local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method`,
        );
      }
    }

    streetMatched += osmMatched;
    console.log(`OSM matching: checked=${osmChecked}, new matches=${osmMatched}`);
  } else {
    console.log('OSM CSV files not found, skipping OSM matching');
    console.log(`  Expected: ${osmOldToCurrentPath}`);
    console.log(`  Expected: ${osmAllStreetsPath}`);
  }

  console.log(`\nStreets TOTAL: matched=${streetMatched}\n`);

  // ========================================
  // Phase 3: Match APARTMENT COMPLEXES
  // ========================================
  console.log('=== Phase 3: Matching APARTMENT COMPLEXES ===');

  // Load our apartment_complexes from DB
  const ourComplexResult = await client.query(
    `SELECT id, name_ru, name_uk, name_normalized, geo_id, source FROM apartment_complexes`,
  );
  const ourComplexes = ourComplexResult.rows;
  console.log(`Our complexes: ${ourComplexes.length} entries`);

  // Build geo descendant sets: for each region (type=2), find all descendant geo IDs
  // Our geo uses nested set (lft/rgt)
  const ourGeoTreeResult = await client.query(
    `SELECT id, lft, rgt, type, parent_id FROM geo`,
  );
  const ourGeoById = new Map<number, { id: number; lft: number; rgt: number; type: string; parent_id: number | null }>();
  for (const g of ourGeoTreeResult.rows) {
    ourGeoById.set(g.id, { id: g.id, lft: g.lft, rgt: g.rgt, type: g.type, parent_id: g.parent_id });
  }

  // Build: region_geo_id → Set<all descendant geo IDs (including self)>
  const regionDescendants = new Map<number, Set<number>>();
  const regions = ourGeoTreeResult.rows.filter((g: any) => g.type === 'region');
  for (const region of regions) {
    const descendants = new Set<number>();
    descendants.add(region.id);
    for (const g of ourGeoTreeResult.rows) {
      if (g.lft > region.lft && g.rgt < region.rgt) {
        descendants.add(g.id);
      }
    }
    regionDescendants.set(region.id, descendants);
  }
  console.log(`Region descendant maps: ${regionDescendants.size} regions`);

  // For each complex geo_id, find which region it belongs to
  function findRegionForGeo(geoId: number): number | null {
    const g = ourGeoById.get(geoId);
    if (!g) return null;
    for (const [regionId, descendants] of regionDescendants) {
      if (descendants.has(geoId)) return regionId;
    }
    return null;
  }

  // Build lookup: normalized_name → [complexes]
  const ourComplexByName = new Map<string, typeof ourComplexes>();
  for (const c of ourComplexes) {
    for (const name of [c.name_normalized, c.name_ru, c.name_uk]) {
      const norm = normalize(name);
      if (!norm) continue;
      const arr = ourComplexByName.get(norm) || [];
      // Avoid duplicates
      if (!arr.find((x: any) => x.id === c.id)) arr.push(c);
      ourComplexByName.set(norm, arr);
    }
  }

  let complexMatched = 0;
  let complexUnmatched = 0;
  let complexGeoFiltered = 0;
  const complexInserts: string[] = [];
  const unmatchedComplexes: string[] = [];

  for (const v2c of vector2Voc33) {
    const kod = parseInt(v2c.kod);
    const nameRu = v2c.name_ru;
    const nameUa = v2c.name_ua;
    const v2GeoId = parseInt(v2c.fk_geo_id);

    const normRu = normalize(nameRu);
    const normUa = normalize(nameUa);

    // Determine allowed geo IDs for this complex
    // Vector2 complexes are linked to regions (type=2), so we need all descendant geos
    const ourRegionGeoId = geoMapping.get(v2GeoId);
    let allowedGeoIds: Set<number> | null = null;
    if (ourRegionGeoId) {
      // If mapped geo is a region, use its descendants
      allowedGeoIds = regionDescendants.get(ourRegionGeoId) || null;
      if (!allowedGeoIds) {
        // Maybe mapped to a city, not a region — find the region for this city
        const regionId = findRegionForGeo(ourRegionGeoId);
        if (regionId) {
          allowedGeoIds = regionDescendants.get(regionId) || null;
        }
        // Also allow exact match
        if (!allowedGeoIds) {
          allowedGeoIds = new Set([ourRegionGeoId]);
        }
      }
    }

    // Find candidates by name
    let candidates = ourComplexByName.get(normRu) || ourComplexByName.get(normUa) || [];
    let matchMethod = 'exact_name';
    let confidence = 1.0;

    // ALWAYS filter by geo (region) if we have geo info
    if (candidates.length > 0 && allowedGeoIds) {
      const geoVerified = candidates.filter(
        (c: any) => c.geo_id && allowedGeoIds!.has(c.geo_id),
      );
      const nullGeo = candidates.filter((c: any) => !c.geo_id);
      if (geoVerified.length > 0) {
        if (geoVerified.length < candidates.length) complexGeoFiltered++;
        candidates = geoVerified;
        matchMethod = 'exact_name_geo';
      } else if (nullGeo.length > 0) {
        // No geo-verified candidates, but there are complexes without geo — accept with lower confidence
        candidates = nullGeo;
        matchMethod = 'name_null_geo';
        confidence = 0.7;
      } else {
        // All candidates are in wrong regions — skip
        candidates = [];
      }
    }

    if (candidates.length === 0) {
      // Try partial match with geo filter
      for (const [normName, entries] of ourComplexByName) {
        if (
          (normRu && normRu.length >= 3 && normName.includes(normRu)) ||
          (normRu && normRu.length >= 3 && normRu.includes(normName) && normName.length >= 3) ||
          (normUa && normUa.length >= 3 && normName.includes(normUa)) ||
          (normUa && normUa.length >= 3 && normUa.includes(normName) && normName.length >= 3)
        ) {
          let filtered = entries;
          if (allowedGeoIds) {
            const geoVerified = entries.filter((c: any) => c.geo_id && allowedGeoIds!.has(c.geo_id));
            const nullGeo = entries.filter((c: any) => !c.geo_id);
            if (geoVerified.length > 0) {
              filtered = geoVerified;
              matchMethod = 'partial_name_geo';
              confidence = 0.7;
            } else if (nullGeo.length > 0) {
              filtered = nullGeo;
              matchMethod = 'partial_null_geo';
              confidence = 0.5;
            } else {
              filtered = [];
            }
          }
          if (filtered.length > 0) {
            candidates = filtered;
            break;
          }
        }
      }
    }

    if (candidates.length >= 1) {
      const best = candidates[0];
      const conf = candidates.length === 1 ? confidence : Math.min(confidence, 0.6);
      const method = candidates.length === 1 ? matchMethod : `${matchMethod}_ambiguous`;
      complexInserts.push(
        `('${SOURCE}', 'complex', ${kod}, ${best.id}, ${conf}, '${method}')`,
      );
      complexMatched++;
    } else {
      complexUnmatched++;
      unmatchedComplexes.push(`${kod}: "${nameRu}"`);
    }
  }

  // Batch insert complex mappings
  if (complexInserts.length > 0) {
    const batchSize = 500;
    for (let i = 0; i < complexInserts.length; i += batchSize) {
      const batch = complexInserts.slice(i, i + batchSize);
      await client.query(
        `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
         VALUES ${batch.join(',')}
         ON CONFLICT (source, entity_type, source_id) DO UPDATE
         SET local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method`,
      );
    }
  }

  console.log(
    `Complexes: matched=${complexMatched}, unmatched=${complexUnmatched}, geoFiltered=${complexGeoFiltered}`,
  );
  if (unmatchedComplexes.length <= 30) {
    console.log('Unmatched complexes:');
    for (const uc of unmatchedComplexes) {
      console.log(`  ${uc}`);
    }
  } else {
    console.log(`First 30 unmatched complexes:`);
    for (const uc of unmatchedComplexes.slice(0, 30)) {
      console.log(`  ${uc}`);
    }
  }

  // ========================================
  // Summary
  // ========================================
  console.log('\n=== SUMMARY ===');
  const totalResult = await client.query(
    `SELECT entity_type, count(*) as total,
            count(*) FILTER (WHERE confidence >= 0.9) as high_conf,
            count(*) FILTER (WHERE confidence < 0.9 AND confidence >= 0.6) as medium_conf,
            count(*) FILTER (WHERE confidence < 0.6) as low_conf
     FROM source_id_mappings
     WHERE source = $1
     GROUP BY entity_type
     ORDER BY entity_type`,
    [SOURCE],
  );

  console.log('\nMappings in source_id_mappings:');
  for (const row of totalResult.rows) {
    console.log(
      `  ${row.entity_type}: total=${row.total}, high=${row.high_conf}, medium=${row.medium_conf}, low=${row.low_conf}`,
    );
  }

  await client.end();
  console.log('\nDone!');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
