/**
 * rematch-complexes-v2.js
 *
 * Complete re-matching of v2 apartment complexes with MANDATORY city verification.
 * Replaces the broken 905 mappings in source_id_mappings (entity_type='complex').
 *
 * Phases:
 *   0. DomRia pre-verified (conf=0.95)
 *   1. Exact name + city (conf=1.00)
 *   2. Base name (no prefix) + city (conf=0.95)
 *   3. Number-aware + city (conf=0.90)
 *   4. Fuzzy Levenshtein ≥0.90 + city (conf=0.85)
 *   5. Coordinates from lookup table (conf=0.80)
 *   6. Create new apartment_complexes (conf=0.75)
 *   Special: exact unique no geo (conf=0.70)
 *
 * Usage: node scripts/rematch-complexes-v2.js
 */

const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ============================================================
// Config
// ============================================================
const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_NAME || 'valuation',
};

const SOURCE = 'vector2_crm';
const TEMP = process.env.TEMP || os.tmpdir();

// ============================================================
// CSV parsing
// ============================================================
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
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

function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCSVLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j].trim()] = (values[j] || '').trim();
    }
    rows.push(row);
  }
  return rows;
}

// ============================================================
// Haversine distance (km)
// ============================================================
function approxDistanceKm(lat1, lng1, lat2, lng2) {
  const latDiff = (lat2 - lat1) * 111.32;
  const lngDiff = (lng2 - lng1) * 111.32 * Math.cos(lat1 * Math.PI / 180);
  return Math.sqrt(latDiff * latDiff + lngDiff * lngDiff);
}

// ============================================================
// Levenshtein distance & similarity
// ============================================================
function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
    }
  }
  return dp[m][n];
}

function similarity(a, b) {
  if (!a || !b) return 0;
  const maxLen = Math.max(a.length, b.length);
  if (maxLen === 0) return 1;
  return 1 - levenshtein(a, b) / maxLen;
}

// ============================================================
// Name normalization
// ============================================================
const PREFIXES = [
  // Long compound first
  'котеджне містечко', 'коттеджный городок',
  'житловий комплекс', 'жилой комплекс',
  'клубний будинок', 'клубный дом',
  'апарт комплекс', 'апарт готель',
  // 3-letter
  'дск', 'сот', 'гск', 'мкр', 'мкрн',
  // 2-letter
  'жк', 'ск', 'ст', 'со', 'жм', 'км', 'кп', 'кб', 'кд', 'жб', 'см', 'кг',
  // Other
  'таунхауси', 'таунхаусы', 'таунхаус',
  'екологічний', 'котеджне',
];

// Garden coop prefixes — separate category
const GARDEN_PREFIXES = ['ск', 'ст', 'дск', 'со', 'сот', 'гск'];

// Cottage prefixes
const COTTAGE_PREFIXES = ['км', 'кп', 'кд', 'кб'];

function normalize(name) {
  if (!name) return '';
  let s = name
    .toLowerCase()
    .replace(/["""«»]/g, '')
    .replace(/[`''ʼ']/g, "'")
    .replace(/[.,;:!?()[\]{}]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  for (const prefix of PREFIXES) {
    if (s.startsWith(prefix + ' ')) {
      s = s.slice(prefix.length).trim();
      break;
    }
  }
  s = s.replace(/^-\s*/, '');
  s = s.replace(/^вул\.?\s*/, '');
  return s.trim();
}

function normalizeAggressive(name) {
  return normalize(name)
    .replace(/[-–—]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Cross-language normalization: Ukrainian → Russian common form
function crossLangNormalize(name) {
  if (!name) return '';
  return name
    .replace(/і/g, 'и')
    .replace(/ї/g, 'и')
    .replace(/є/g, 'е')
    .replace(/ґ/g, 'г')
    .replace(/'/g, '')
    .trim();
}

// Translation table for known equivalents
const TRANSLATIONS = {
  'перлина': 'жемчужина',
  'жемчужина': 'перлина',
  'зірка': 'звезда',
  'звезда': 'зірка',
  'сонячний': 'солнечный',
  'солнечный': 'сонячний',
  'затишок': 'уют',
  'уют': 'затишок',
  'мрія': 'мечта',
  'мечта': 'мрія',
  'щасливий': 'счастливый',
  'вишневий': 'вишневый',
  'калинова': 'калиновая',
  'зелений': 'зеленый',
  'золотий': 'золотой',
  'срібний': 'серебряный',
  'центральний': 'центральный',
  'комфорт': 'комфорт',
  'престиж': 'престиж',
};

// ============================================================
// Number extraction from names
// ============================================================
const ORDINAL_UNITS = {
  'перша': 1, 'першій': 1, 'першая': 1, 'первая': 1,
  'друга': 2, 'другій': 2, 'другая': 2, 'вторая': 2,
  'третя': 3, 'третій': 3, 'третья': 3,
  'четверта': 4, 'четвертий': 4, 'четвертая': 4,
  "п'ята": 5, 'пята': 5, 'пятая': 5, "п'ятий": 5,
  'шоста': 6, 'шостий': 6, 'шестая': 6, 'шеста': 6,
  'сьома': 7, 'сьомий': 7, 'седьмая': 7, 'сома': 7,
  'восьма': 8, 'восьмий': 8, 'восьмая': 8,
  "дев'ята": 9, 'девята': 9, 'девятий': 9, 'девятая': 9,
};

const ORDINAL_TEENS = {
  'десята': 10, 'десятий': 10, 'десятая': 10,
  'одинадцята': 11, 'одиннадцята': 11, 'одинадцятий': 11,
  'дванадцята': 12, 'двінадцята': 12, 'дванадцятий': 12, 'двенадцята': 12,
  'тринадцята': 13, 'тринадцятий': 13,
  'чотирнадцята': 14, 'чотирнадцятий': 14,
  "п'ятнадцята": 15, 'пятнадцята': 15, "п'ятнадцятий": 15,
  'шістнадцята': 16, 'шістнадцятий': 16, 'шистнадцята': 16, 'шіснадцята': 16,
  'сімнадцята': 17, 'сімнадцятий': 17,
  'вісімнадцята': 18, 'вісімнадцятий': 18,
  "дев'ятнадцята": 19, 'девятнадцята': 19, "дев'ятнадцятий": 19,
};

const ORDINAL_TENS = {
  'двадцять': 20, 'двадцята': 20, 'двадцятий': 20,
  'тридцять': 30, 'тридцята': 30, 'тридцятий': 30,
  'сорок': 40, 'сорокова': 40, 'сороковий': 40,
  "п'ятдесят": 50, "п'ятьдесят": 50, 'пятдесят': 50, 'пятьдесят': 50, 'пятидесят': 50,
  "п'ятидесята": 50, "п'ятьдесята": 50, "п'ятдесята": 50, 'пятидесята': 50, 'пятьдесята': 50, 'пятдесята': 50,
  'шістдесят': 60, 'шістьдесят': 60, 'шістдесять': 60, 'шістьдесять': 60,
  'шістдесята': 60, 'шістьдесята': 60, 'шестидесята': 60, 'шестідесята': 60,
};

// Russian ordinal units (for "Жемчужина первая" etc.)
const RUSSIAN_ORDINAL_UNITS = {
  'первая': 1, 'первый': 1, 'первое': 1,
  'вторая': 2, 'второй': 2, 'второе': 2,
  'третья': 3, 'третий': 3, 'третье': 3,
  'четвертая': 4, 'четвертый': 4, 'четвертое': 4,
  'пятая': 5, 'пятый': 5, 'пятое': 5,
  'шестая': 6, 'шестой': 6, 'шестое': 6,
  'седьмая': 7, 'седьмой': 7, 'седьмое': 7,
  'восьмая': 8, 'восьмой': 8, 'восьмое': 8,
  'девятая': 9, 'девятый': 9, 'девятое': 9,
  'десятая': 10, 'десятый': 10, 'десятое': 10,
};

function parseOrdinalWords(text) {
  const words = text.trim().split(/\s+/);
  if (words.length === 1) {
    const w = words[0];
    if (ORDINAL_TEENS[w] !== undefined) return ORDINAL_TEENS[w];
    if (ORDINAL_TENS[w] !== undefined) return ORDINAL_TENS[w];
    if (ORDINAL_UNITS[w] !== undefined) return ORDINAL_UNITS[w];
    if (RUSSIAN_ORDINAL_UNITS[w] !== undefined) return RUSSIAN_ORDINAL_UNITS[w];
    return null;
  }
  if (words.length === 2) {
    const tens = ORDINAL_TENS[words[0]];
    const units = ORDINAL_UNITS[words[1]] || RUSSIAN_ORDINAL_UNITS[words[1]];
    if (tens !== undefined && units !== undefined) return tens + units;
    const tens2 = ORDINAL_TENS[words[1]];
    const units2 = ORDINAL_UNITS[words[0]] || RUSSIAN_ORDINAL_UNITS[words[0]];
    if (tens2 !== undefined && units2 !== undefined) return tens2 + units2;
    return null;
  }
  return null;
}

function isOrdinalWord(w) {
  return ORDINAL_UNITS[w] !== undefined ||
    ORDINAL_TEENS[w] !== undefined ||
    ORDINAL_TENS[w] !== undefined ||
    RUSSIAN_ORDINAL_UNITS[w] !== undefined;
}

function extractNameParts(normalized) {
  const s = normalized;
  if (!s) return { baseName: '', numberValue: null };

  // 1. Trailing digit suffix: "акварель 4", "грінвуд-2"
  const trailingNum = s.match(/^(.+?)[\s\-–—]+(\d+)\+?$/);
  if (trailingNum) {
    return { baseName: trailingNum[1].trim(), numberValue: parseInt(trailingNum[2]) };
  }

  // 2. Leading digit: "32 перлина", "6-та перлина"
  const leadingNum = s.match(/^(\d+)(?:-?(?:та|га|ша|тя|ма|ій|й|а|я))?\s+(.+)$/);
  if (leadingNum) {
    return { baseName: leadingNum[2].trim(), numberValue: parseInt(leadingNum[1]) };
  }

  // 3. Ordinal words
  const words = s.split(/\s+/);
  if (words.length >= 2) {
    const lastWord = words[words.length - 1];

    // 2-word ordinal at start
    if (words.length >= 3) {
      const ordinalCandidate = words.slice(0, -1).join(' ');
      const num = parseOrdinalWords(ordinalCandidate);
      if (num !== null) return { baseName: lastWord, numberValue: num };
    }

    // 1-word ordinal at start
    if (isOrdinalWord(words[0])) {
      const num = parseOrdinalWords(words[0]);
      if (num !== null) return { baseName: words.slice(1).join(' '), numberValue: num };
    }

    // Ordinal at end (2-word)
    if (words.length >= 3) {
      const ordinalEnd = words.slice(-2).join(' ');
      const num = parseOrdinalWords(ordinalEnd);
      if (num !== null) return { baseName: words.slice(0, -2).join(' '), numberValue: num };
    }

    // Ordinal at end (1-word)
    const lastOrd = parseOrdinalWords(words[words.length - 1]);
    if (lastOrd !== null) {
      return { baseName: words.slice(0, -1).join(' '), numberValue: lastOrd };
    }
  }

  return { baseName: s, numberValue: null };
}

// ============================================================
// Category classification
// ============================================================
function classifyV2Complex(name, typeSk) {
  const lower = (name || '').toLowerCase().trim();
  const prefix = (typeSk || '').toUpperCase().trim();

  // Check by type_sk field first
  if (['СК', 'СТ', 'ДСК', 'СО', 'СОТ', 'ГСК'].includes(prefix)) {
    return 'garden_coop';
  }
  if (['КМ', 'КП', 'КД', 'КБ'].includes(prefix)) {
    return 'cottage';
  }

  // Fallback: check name prefix
  for (const gp of GARDEN_PREFIXES) {
    if (lower.startsWith(gp + ' ')) return 'garden_coop';
  }
  for (const cp of COTTAGE_PREFIXES) {
    if (lower.startsWith(cp + ' ')) return 'cottage';
  }

  return 'zhk';
}

// ============================================================
// Main
// ============================================================
async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to DB:', `${DB_CONFIG.host}:${DB_CONFIG.port}/${DB_CONFIG.database}`);

  // ========================================
  // Section 0: Load all data
  // ========================================
  console.log('\n=== Section 0: Loading data ===');

  // 0.1 Load v2 CSV
  const voc33Path = path.join(TEMP, 'vector2_voc33.csv');
  const v2Rows = parseCSV(voc33Path);
  console.log(`Loaded ${v2Rows.length} v2 complexes from CSV`);

  const v2Complexes = v2Rows.map(r => ({
    kod: parseInt(r.kod),
    nameRu: (r.name_ru || '').trim(),
    nameUa: (r.name_ua || '').trim(),
    geoId: parseInt(r.fk_geo_id) || null,
    typeSk: (r.type_sk || '').trim(),
    category: classifyV2Complex(r.name_ru || r.name_ua, r.type_sk),
  }));

  // 0.2 Load our apartment_complexes
  const acResult = await client.query(
    `SELECT id, name_ru, name_uk, name_en, name_normalized, lat, lng, geo_id, source FROM apartment_complexes`
  );
  const ourComplexes = acResult.rows.map(r => ({
    id: r.id,
    nameRu: r.name_ru || '',
    nameUk: r.name_uk || '',
    nameEn: r.name_en || '',
    nameNormalized: r.name_normalized || '',
    lat: r.lat ? parseFloat(r.lat) : null,
    lng: r.lng ? parseFloat(r.lng) : null,
    geoId: r.geo_id,
    source: r.source,
    // Will be filled later
    cityId: null,
    normRu: '',
    normUk: '',
    normAggrRu: '',
    normAggrUk: '',
    crossRu: '',
    crossUk: '',
    partsRu: null,
    partsUk: null,
  }));
  console.log(`Loaded ${ourComplexes.length} apartment_complexes`);

  // 0.3 Load geo tree
  const geoResult = await client.query(
    `SELECT id, name, type, parent_id, lft, rgt, lvl, lat, lng FROM geo`
  );
  const geoRows = geoResult.rows;
  const geoById = new Map();
  for (const g of geoRows) {
    geoById.set(g.id, {
      id: g.id,
      name: g.name,
      type: g.type,
      parentId: g.parent_id,
      lft: g.lft,
      rgt: g.rgt,
      lvl: g.lvl,
      lat: g.lat ? parseFloat(g.lat) : null,
      lng: g.lng ? parseFloat(g.lng) : null,
    });
  }
  console.log(`Loaded ${geoById.size} geo entries`);

  // 0.4 Load v2 geo mappings
  const geoMappingResult = await client.query(
    `SELECT source_id, local_id FROM source_id_mappings WHERE source = '${SOURCE}' AND entity_type = 'geo'`
  );
  const v2GeoToOurGeo = new Map();
  for (const r of geoMappingResult.rows) {
    v2GeoToOurGeo.set(r.source_id, r.local_id);
  }
  console.log(`Loaded ${v2GeoToOurGeo.size} v2→our geo mappings`);

  // 0.5 Load lookup table & domria matches
  const lookupTablePath = path.join(TEMP, 'complex_lookup_table.json');
  const lookupTable = JSON.parse(fs.readFileSync(lookupTablePath, 'utf8'));
  console.log(`Loaded ${lookupTable.length} lookup table entries (${lookupTable.filter(e => e.lat && e.lng).length} with coords)`);

  const domriaPath = path.join(TEMP, 'domria_clean_matches.json');
  const domriaMatches = JSON.parse(fs.readFileSync(domriaPath, 'utf8'));
  console.log(`Loaded ${domriaMatches.length} domria matches`);

  // Build lookup table index by kod
  const lookupByKod = new Map();
  for (const entry of lookupTable) {
    lookupByKod.set(entry.kod, entry);
  }

  // ========================================
  // Section 1: Build geo helper functions
  // ========================================
  console.log('\n=== Section 1: Building geo indexes ===');

  // Build parent→children map (nested set lft/rgt is broken, use parent_id)
  const childrenMap = new Map(); // parentId → [childGeo]
  for (const g of geoRows) {
    if (g.parent_id) {
      if (!childrenMap.has(g.parent_id)) childrenMap.set(g.parent_id, []);
      childrenMap.get(g.parent_id).push(g);
    }
  }

  // Get ALL descendants of a geo via BFS on parent_id
  function getAllDescendantIds(geoId) {
    const result = new Set();
    const queue = [geoId];
    while (queue.length > 0) {
      const current = queue.shift();
      const children = childrenMap.get(current) || [];
      for (const child of children) {
        if (!result.has(child.id)) {
          result.add(child.id);
          queue.push(child.id);
        }
      }
    }
    return result;
  }

  // Find all cities
  const cities = geoRows.filter(g => g.type === 'city');
  const cityList = cities.map(c => ({
    id: c.id,
    lat: c.lat ? parseFloat(c.lat) : null,
    lng: c.lng ? parseFloat(c.lng) : null,
    name: c.name,
    parentId: c.parent_id,
  }));
  console.log(`Cities: ${cityList.length}`);

  // geoToCityId: for any geo_id, find which city it belongs to
  // city → itself; city_district → parent city; otherwise null
  function geoToCityId(geoId) {
    const g = geoById.get(geoId);
    if (!g) return null;

    if (g.type === 'city') return g.id;

    if (g.type === 'city_district') {
      // Walk up parent_id to find city
      let current = g;
      for (let i = 0; i < 5; i++) {
        if (!current.parentId) return null;
        const parent = geoById.get(current.parentId);
        if (!parent) return null;
        if (parent.type === 'city') return parent.id;
        current = parent;
      }
      return null;
    }

    // village, region_district, region — not inside a city
    return null;
  }

  // resolveV2GeoToCityIds: given a v2 geo_id, find ALL allowed geo IDs
  // (cities + their city_districts + the region/district itself)
  function resolveV2GeoToCityIds(v2GeoId) {
    if (!v2GeoId) return new Set();

    const ourGeoId = v2GeoToOurGeo.get(v2GeoId);
    if (!ourGeoId) return new Set();

    const g = geoById.get(ourGeoId);
    if (!g) return new Set();

    const result = new Set();
    result.add(g.id);

    if (g.type === 'city') {
      // Add all city_districts under this city
      const descendants = getAllDescendantIds(g.id);
      for (const id of descendants) result.add(id);
      return result;
    }

    if (g.type === 'city_district') {
      // Find parent city, add city + all its districts
      const cityId = geoToCityId(g.id);
      if (cityId) {
        result.add(cityId);
        const siblings = getAllDescendantIds(cityId);
        for (const id of siblings) result.add(id);
      }
      return result;
    }

    // region or region_district → find ALL descendants (cities, districts, villages...)
    const descendants = getAllDescendantIds(g.id);
    for (const id of descendants) result.add(id);
    return result;
  }

  // For our complexes: determine city ID
  // If geo_id exists → use geoToCityId
  // If geo_id is NULL but coords exist → haversine to nearest city
  const citiesWithCoords = cityList.filter(c => c.lat && c.lng);

  function findNearestCity(lat, lng) {
    let bestDist = Infinity;
    let bestCity = null;
    for (const city of citiesWithCoords) {
      const dist = approxDistanceKm(lat, lng, city.lat, city.lng);
      if (dist < bestDist) {
        bestDist = dist;
        bestCity = city;
      }
    }
    // Only accept if within 50km
    return bestDist < 50 ? bestCity : null;
  }

  // Assign cityId to all our complexes
  let assignedByGeo = 0, assignedByCoords = 0, unassigned = 0;
  for (const c of ourComplexes) {
    if (c.geoId) {
      const cityId = geoToCityId(c.geoId);
      c.cityId = cityId || c.geoId; // fallback to geoId itself
      assignedByGeo++;
    } else if (c.lat && c.lng) {
      const city = findNearestCity(c.lat, c.lng);
      c.cityId = city ? city.id : null;
      if (city) assignedByCoords++;
      else unassigned++;
    } else {
      unassigned++;
    }
  }
  console.log(`Our complexes city assignment: byGeo=${assignedByGeo}, byCoords=${assignedByCoords}, unassigned=${unassigned}`);

  // ========================================
  // Section 3: Normalize names & build indexes
  // ========================================
  console.log('\n=== Section 3: Building name indexes ===');

  for (const c of ourComplexes) {
    c.normRu = normalize(c.nameRu);
    c.normUk = normalize(c.nameUk);
    c.normAggrRu = normalizeAggressive(c.nameRu);
    c.normAggrUk = normalizeAggressive(c.nameUk);
    c.crossRu = crossLangNormalize(c.normRu);
    c.crossUk = crossLangNormalize(c.normUk);
    c.partsRu = extractNameParts(c.normRu);
    c.partsUk = extractNameParts(c.normUk);
  }

  // ourByNormalized: normalized name → [complexes]
  const ourByNormalized = new Map();
  function addToIndex(map, key, value) {
    if (!key) return;
    if (!map.has(key)) map.set(key, []);
    const arr = map.get(key);
    if (!arr.find(e => e.id === value.id)) arr.push(value);
  }

  for (const c of ourComplexes) {
    addToIndex(ourByNormalized, c.normRu, c);
    addToIndex(ourByNormalized, c.normUk, c);
    addToIndex(ourByNormalized, c.normAggrRu, c);
    addToIndex(ourByNormalized, c.normAggrUk, c);
    addToIndex(ourByNormalized, c.crossRu, c);
    addToIndex(ourByNormalized, c.crossUk, c);
    // Also add translations
    for (const norm of [c.normRu, c.normUk]) {
      if (norm && TRANSLATIONS[norm]) {
        addToIndex(ourByNormalized, TRANSLATIONS[norm], c);
      }
    }
  }
  console.log(`ourByNormalized: ${ourByNormalized.size} unique keys`);

  // ourByBaseName: baseName → [complexes]
  const ourByBaseName = new Map();
  for (const c of ourComplexes) {
    if (c.partsRu) addToIndex(ourByBaseName, c.partsRu.baseName, c);
    if (c.partsUk) addToIndex(ourByBaseName, c.partsUk.baseName, c);
    // Cross-lang base names
    if (c.partsRu) addToIndex(ourByBaseName, crossLangNormalize(c.partsRu.baseName), c);
    if (c.partsUk) addToIndex(ourByBaseName, crossLangNormalize(c.partsUk.baseName), c);
    // Translation base names
    if (c.partsRu && TRANSLATIONS[c.partsRu.baseName]) {
      addToIndex(ourByBaseName, TRANSLATIONS[c.partsRu.baseName], c);
    }
    if (c.partsUk && TRANSLATIONS[c.partsUk.baseName]) {
      addToIndex(ourByBaseName, TRANSLATIONS[c.partsUk.baseName], c);
    }
  }
  console.log(`ourByBaseName: ${ourByBaseName.size} unique keys`);

  // ourByCityId: cityId → [complexes]
  const ourByCityId = new Map();
  for (const c of ourComplexes) {
    if (c.cityId) addToIndex(ourByCityId, c.cityId, c);
  }
  console.log(`ourByCityId: ${ourByCityId.size} cities`);

  // Base name counts (for series detection)
  const baseNameCounts = new Map();
  for (const c of ourComplexes) {
    if (c.partsRu) {
      const b = c.partsRu.baseName;
      baseNameCounts.set(b, (baseNameCounts.get(b) || 0) + 1);
    }
    if (c.partsUk) {
      const b = c.partsUk.baseName;
      baseNameCounts.set(b, (baseNameCounts.get(b) || 0) + 1);
    }
  }

  // Show series names
  const seriesNames = [...baseNameCounts.entries()]
    .filter(([, cnt]) => cnt >= 3)
    .sort((a, b) => b[1] - a[1]);
  console.log(`\nSeries names (base shared by ≥3): ${seriesNames.length}`);
  for (const [name, cnt] of seriesNames.slice(0, 15)) {
    console.log(`  "${name}": ${cnt} complexes`);
  }

  // City verification function
  // v2AllowedGeoIds contains ALL geo IDs (cities + districts + villages) in the v2 region
  // ourComplex has .cityId (resolved city) and .geoId (direct geo_id from DB)
  function isCityVerified(v2AllowedGeoIds, ourComplex) {
    if (!v2AllowedGeoIds || v2AllowedGeoIds.size === 0) return false;

    // Direct geoId match (complex geo_id is in the allowed set)
    if (ourComplex.geoId && v2AllowedGeoIds.has(ourComplex.geoId)) return true;

    // CityId match (resolved city is in the allowed set)
    if (ourComplex.cityId && v2AllowedGeoIds.has(ourComplex.cityId)) return true;

    return false;
  }

  // Helper: get best geo-specificity from candidates
  function selectBestGeoCandidate(candidates, v2AllowedGeoIds) {
    if (candidates.length === 1) return candidates[0];
    // Prefer the one with most specific geo match (city_district > city > region)
    let best = candidates[0];
    let bestScore = 0;
    for (const c of candidates) {
      let score = 0;
      if (c.geoId) {
        const g = geoById.get(c.geoId);
        if (g) {
          if (g.type === 'city_district') score = 3;
          else if (g.type === 'city') score = 2;
          else score = 1;
        }
      }
      if (score > bestScore) {
        bestScore = score;
        best = c;
      }
    }
    return best;
  }

  // ========================================
  // Matching phases
  // ========================================
  const results = []; // { v2Kod, localId, confidence, method, v2Name, ourName, phase }
  const matched = new Set(); // v2 kods matched
  const manualReview = []; // borderline cases
  const gardenCoops = []; // skipped

  // Classify & skip garden coops
  const gardenCoopCount = v2Complexes.filter(v => v.category === 'garden_coop').length;
  const cottageCount = v2Complexes.filter(v => v.category === 'cottage').length;
  const zhkCount = v2Complexes.filter(v => v.category === 'zhk').length;
  console.log(`\nV2 categories: zhk=${zhkCount}, cottage=${cottageCount}, garden_coop=${gardenCoopCount}`);

  for (const v2 of v2Complexes) {
    if (v2.category === 'garden_coop') {
      gardenCoops.push(v2);
      matched.add(v2.kod);
    }
  }

  // Pre-compute v2 allowedGeoIds for each v2 complex
  const v2AllowedGeoCache = new Map();
  for (const v2 of v2Complexes) {
    if (v2.geoId) {
      v2AllowedGeoCache.set(v2.kod, resolveV2GeoToCityIds(v2.geoId));
    }
  }

  // ========================================
  // Phase 0: DomRia pre-verified
  // ========================================
  console.log('\n=== Phase 0: DomRia pre-verified ===');

  // DomRia matches have domRiaName but not our ID — match by name to our complexes
  for (const dm of domriaMatches) {
    if (matched.has(dm.v2Kod)) continue;

    const normDomria = normalize(dm.domRiaName);
    const candidates = ourByNormalized.get(normDomria) || [];

    if (candidates.length > 0) {
      // If we have v2 geo info, try to city-verify
      const v2Allowed = v2AllowedGeoCache.get(dm.v2Kod);
      let cityVerified = candidates.filter(c => v2Allowed && isCityVerified(v2Allowed, c));

      const chosen = cityVerified.length > 0
        ? selectBestGeoCandidate(cityVerified, v2Allowed)
        : candidates[0]; // DomRia is already verified, accept even without geo

      results.push({
        v2Kod: dm.v2Kod,
        localId: chosen.id,
        confidence: 0.95,
        method: 'domria_verified',
        v2Name: dm.v2Name,
        ourName: chosen.nameUk || chosen.nameRu,
        phase: 0,
      });
      matched.add(dm.v2Kod);
    }
  }
  console.log(`Phase 0 matches: ${results.filter(r => r.phase === 0).length}`);

  // ========================================
  // Phase 1: Exact name + city
  // ========================================
  console.log('\n=== Phase 1: Exact name + city ===');
  let phase1Count = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;

    const normRu = normalize(v2.nameRu);
    const normUa = normalize(v2.nameUa);
    const crossRu = crossLangNormalize(normRu);
    const crossUa = crossLangNormalize(normUa);

    // Gather all candidates from exact normalized match
    const candidateSet = new Set();
    const allCandidates = [];
    for (const key of [normRu, normUa, crossRu, crossUa]) {
      const list = ourByNormalized.get(key);
      if (list) {
        for (const c of list) {
          if (!candidateSet.has(c.id)) {
            candidateSet.add(c.id);
            allCandidates.push(c);
          }
        }
      }
    }
    // Also try translations
    for (const key of [normRu, normUa]) {
      if (key && TRANSLATIONS[key]) {
        const list = ourByNormalized.get(TRANSLATIONS[key]);
        if (list) {
          for (const c of list) {
            if (!candidateSet.has(c.id)) {
              candidateSet.add(c.id);
              allCandidates.push(c);
            }
          }
        }
      }
    }

    if (allCandidates.length === 0) continue;

    // City verify
    const v2Allowed = v2AllowedGeoCache.get(v2.kod);
    if (!v2Allowed || v2Allowed.size === 0) continue; // No geo → skip for Phase 1

    const cityVerified = allCandidates.filter(c => isCityVerified(v2Allowed, c));
    if (cityVerified.length === 0) continue;

    const best = selectBestGeoCandidate(cityVerified, v2Allowed);

    results.push({
      v2Kod: v2.kod,
      localId: best.id,
      confidence: 1.00,
      method: 'exact_city',
      v2Name: v2.nameRu || v2.nameUa,
      ourName: best.nameUk || best.nameRu,
      phase: 1,
    });
    matched.add(v2.kod);
    phase1Count++;
  }
  console.log(`Phase 1 matches: ${phase1Count}`);

  // ========================================
  // Phase 2: Base name (stripped prefix) + city
  // ========================================
  console.log('\n=== Phase 2: Base name + city ===');
  let phase2Count = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;

    const normRu = normalize(v2.nameRu);
    const normUa = normalize(v2.nameUa);
    const aggrRu = normalizeAggressive(v2.nameRu);
    const aggrUa = normalizeAggressive(v2.nameUa);
    const crossRu = crossLangNormalize(normRu);
    const crossUa = crossLangNormalize(normUa);
    const crossAggrRu = crossLangNormalize(aggrRu);
    const crossAggrUa = crossLangNormalize(aggrUa);

    // Try all variants
    const candidateSet = new Set();
    const allCandidates = [];
    for (const key of [normRu, normUa, aggrRu, aggrUa, crossRu, crossUa, crossAggrRu, crossAggrUa]) {
      const list = ourByNormalized.get(key);
      if (list) {
        for (const c of list) {
          if (!candidateSet.has(c.id)) {
            candidateSet.add(c.id);
            allCandidates.push(c);
          }
        }
      }
      // Translations
      if (key && TRANSLATIONS[key]) {
        const tList = ourByNormalized.get(TRANSLATIONS[key]);
        if (tList) {
          for (const c of tList) {
            if (!candidateSet.has(c.id)) {
              candidateSet.add(c.id);
              allCandidates.push(c);
            }
          }
        }
      }
    }

    if (allCandidates.length === 0) continue;

    // City verify
    const v2Allowed = v2AllowedGeoCache.get(v2.kod);
    if (!v2Allowed || v2Allowed.size === 0) continue;

    const cityVerified = allCandidates.filter(c => isCityVerified(v2Allowed, c));
    if (cityVerified.length === 0) continue;

    const best = selectBestGeoCandidate(cityVerified, v2Allowed);

    results.push({
      v2Kod: v2.kod,
      localId: best.id,
      confidence: 0.95,
      method: 'base_name_city',
      v2Name: v2.nameRu || v2.nameUa,
      ourName: best.nameUk || best.nameRu,
      phase: 2,
    });
    matched.add(v2.kod);
    phase2Count++;
  }
  console.log(`Phase 2 matches: ${phase2Count}`);

  // ========================================
  // Phase 3: Number-aware + city
  // ========================================
  console.log('\n=== Phase 3: Number-aware + city ===');
  let phase3Count = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;

    const normRu = normalize(v2.nameRu);
    const normUa = normalize(v2.nameUa);
    const v2PartsRu = extractNameParts(normRu);
    const v2PartsUa = extractNameParts(normUa);

    // Need at least a baseName
    if (!v2PartsRu.baseName && !v2PartsUa.baseName) continue;

    // Gather candidates by base name
    const candidateSet = new Set();
    const allCandidates = [];
    for (const parts of [v2PartsRu, v2PartsUa]) {
      if (!parts.baseName) continue;
      for (const key of [parts.baseName, crossLangNormalize(parts.baseName)]) {
        const list = ourByBaseName.get(key);
        if (list) {
          for (const c of list) {
            if (!candidateSet.has(c.id)) {
              candidateSet.add(c.id);
              allCandidates.push(c);
            }
          }
        }
        // Also check translations
        if (TRANSLATIONS[key]) {
          const tList = ourByBaseName.get(TRANSLATIONS[key]);
          if (tList) {
            for (const c of tList) {
              if (!candidateSet.has(c.id)) {
                candidateSet.add(c.id);
                allCandidates.push(c);
              }
            }
          }
        }
      }
    }

    if (allCandidates.length === 0) continue;

    // City verify
    const v2Allowed = v2AllowedGeoCache.get(v2.kod);
    if (!v2Allowed || v2Allowed.size === 0) continue;

    const cityVerified = allCandidates.filter(c => isCityVerified(v2Allowed, c));
    if (cityVerified.length === 0) continue;

    // Number-aware matching
    let bestMatch = null;
    for (const candidate of cityVerified) {
      for (const v2Parts of [v2PartsRu, v2PartsUa]) {
        if (!v2Parts.baseName) continue;

        for (const ourParts of [candidate.partsRu, candidate.partsUk]) {
          if (!ourParts) continue;

          // Check base name similarity
          const baseSim = Math.max(
            similarity(v2Parts.baseName, ourParts.baseName),
            similarity(crossLangNormalize(v2Parts.baseName), crossLangNormalize(ourParts.baseName)),
          );
          if (baseSim < 0.85) continue;

          // Check number compatibility
          const v2BaseCount = baseNameCounts.get(v2Parts.baseName) || baseNameCounts.get(crossLangNormalize(v2Parts.baseName)) || 0;
          const ourBaseCount = baseNameCounts.get(ourParts.baseName) || baseNameCounts.get(crossLangNormalize(ourParts.baseName)) || 0;
          const isSeries = Math.max(v2BaseCount, ourBaseCount) >= 3;

          if (isSeries) {
            // Series: numbers must match exactly
            if (v2Parts.numberValue !== null && ourParts.numberValue !== null) {
              if (v2Parts.numberValue === ourParts.numberValue) {
                bestMatch = candidate;
                break;
              }
              // Different number in series → REJECT this candidate
              continue;
            }
            // One has number, other doesn't → in series this is ambiguous, skip
            if (v2Parts.numberValue !== null || ourParts.numberValue !== null) continue;
            // Neither has number → OK
            bestMatch = candidate;
            break;
          } else {
            // Unique name: numbers are just phases, accept any
            bestMatch = candidate;
            break;
          }
        }
        if (bestMatch) break;
      }
      if (bestMatch) break;
    }

    if (bestMatch) {
      results.push({
        v2Kod: v2.kod,
        localId: bestMatch.id,
        confidence: 0.90,
        method: 'number_aware_city',
        v2Name: v2.nameRu || v2.nameUa,
        ourName: bestMatch.nameUk || bestMatch.nameRu,
        phase: 3,
      });
      matched.add(v2.kod);
      phase3Count++;
    }
  }
  console.log(`Phase 3 matches: ${phase3Count}`);

  // ========================================
  // Phase 4: Fuzzy Levenshtein ≥0.90 + city
  // ========================================
  console.log('\n=== Phase 4: Fuzzy + city ===');
  let phase4Count = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;

    const v2Allowed = v2AllowedGeoCache.get(v2.kod);
    if (!v2Allowed || v2Allowed.size === 0) continue;

    const normRu = normalize(v2.nameRu);
    const normUa = normalize(v2.nameUa);
    const aggrRu = normalizeAggressive(v2.nameRu);
    const aggrUa = normalizeAggressive(v2.nameUa);
    const crossRu = crossLangNormalize(normRu);
    const crossUa = crossLangNormalize(normUa);

    if (!normRu && !normUa) continue;

    // Only search within same city to keep it fast
    const cityCandidates = [];
    for (const geoId of v2Allowed) {
      const cityId = geoToCityId(geoId);
      if (cityId) {
        const list = ourByCityId.get(cityId);
        if (list) {
          for (const c of list) {
            if (!cityCandidates.find(e => e.id === c.id)) {
              cityCandidates.push(c);
            }
          }
        }
      }
    }
    // Also get complexes directly by the allowed geoIds
    for (const geoId of v2Allowed) {
      const list = ourByCityId.get(geoId);
      if (list) {
        for (const c of list) {
          if (!cityCandidates.find(e => e.id === c.id)) {
            cityCandidates.push(c);
          }
        }
      }
    }

    if (cityCandidates.length === 0) continue;

    let bestSim = 0;
    let bestCandidate = null;

    for (const candidate of cityCandidates) {
      // Calculate max similarity across all name variants
      let maxSim = 0;
      const v2Names = [normRu, normUa, aggrRu, aggrUa, crossRu, crossUa].filter(Boolean);
      const ourNames = [candidate.normRu, candidate.normUk, candidate.normAggrRu, candidate.normAggrUk, candidate.crossRu, candidate.crossUk].filter(Boolean);

      for (const v2n of v2Names) {
        for (const on of ourNames) {
          const s = similarity(v2n, on);
          if (s > maxSim) maxSim = s;
        }
      }

      if (maxSim > bestSim) {
        bestSim = maxSim;
        bestCandidate = candidate;
      }
    }

    if (!bestCandidate) continue;

    // Number check
    const v2PartsRu = extractNameParts(normRu);
    const v2PartsUa = extractNameParts(normUa);
    let numberOk = false;
    for (const v2Parts of [v2PartsRu, v2PartsUa]) {
      for (const ourParts of [bestCandidate.partsRu, bestCandidate.partsUk]) {
        if (!ourParts) continue;
        const v2BaseCount = baseNameCounts.get(v2Parts.baseName) || 0;
        const ourBaseCount = baseNameCounts.get(ourParts.baseName) || 0;
        const isSeries = Math.max(v2BaseCount, ourBaseCount) >= 3;

        if (!isSeries) { numberOk = true; break; }
        if (v2Parts.numberValue === null && ourParts.numberValue === null) { numberOk = true; break; }
        if (v2Parts.numberValue !== null && ourParts.numberValue !== null && v2Parts.numberValue === ourParts.numberValue) { numberOk = true; break; }
      }
      if (numberOk) break;
    }

    if (bestSim >= 0.90 && numberOk) {
      results.push({
        v2Kod: v2.kod,
        localId: bestCandidate.id,
        confidence: 0.85,
        method: 'fuzzy_city',
        v2Name: v2.nameRu || v2.nameUa,
        ourName: bestCandidate.nameUk || bestCandidate.nameRu,
        phase: 4,
      });
      matched.add(v2.kod);
      phase4Count++;
    } else if (bestSim >= 0.80 && bestSim < 0.90) {
      manualReview.push({
        v2Kod: v2.kod,
        v2Name: v2.nameRu || v2.nameUa,
        ourId: bestCandidate.id,
        ourName: bestCandidate.nameUk || bestCandidate.nameRu,
        similarity: Math.round(bestSim * 100) / 100,
        numberOk,
      });
    }
  }
  console.log(`Phase 4 matches: ${phase4Count}`);
  console.log(`Phase 4 manual review: ${manualReview.length}`);

  // ========================================
  // Phase 5: Coordinates from lookup table
  // ========================================
  console.log('\n=== Phase 5: Coordinate match ===');
  let phase5Count = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;

    const lookup = lookupByKod.get(v2.kod);
    if (!lookup || !lookup.lat || !lookup.lng) continue;

    // Find our complexes within 1km
    const nearby = [];
    for (const c of ourComplexes) {
      if (!c.lat || !c.lng) continue;
      const dist = approxDistanceKm(lookup.lat, lookup.lng, c.lat, c.lng);
      if (dist <= 1.0) {
        nearby.push({ complex: c, distance: dist });
      }
    }

    if (nearby.length === 0) continue;

    // Sort by distance
    nearby.sort((a, b) => a.distance - b.distance);

    // Check name similarity for nearby complexes
    const lookupNorm = normalize(lookup.name || v2.nameRu || v2.nameUa);
    const lookupCross = crossLangNormalize(lookupNorm);
    let bestMatch = null;

    for (const { complex: c, distance } of nearby) {
      const maxSim = Math.max(
        similarity(lookupNorm, c.normRu),
        similarity(lookupNorm, c.normUk),
        similarity(lookupCross, c.crossRu),
        similarity(lookupCross, c.crossUk),
        similarity(normalize(v2.nameRu), c.normRu),
        similarity(normalize(v2.nameRu), c.normUk),
        similarity(normalize(v2.nameUa), c.normRu),
        similarity(normalize(v2.nameUa), c.normUk),
      );

      if (maxSim >= 0.70 && distance < 0.5) {
        bestMatch = c;
        break;
      }
      if (maxSim >= 0.80 && distance < 1.0) {
        bestMatch = c;
        break;
      }
    }

    if (bestMatch) {
      results.push({
        v2Kod: v2.kod,
        localId: bestMatch.id,
        confidence: 0.80,
        method: 'coordinate_match',
        v2Name: v2.nameRu || v2.nameUa,
        ourName: bestMatch.nameUk || bestMatch.nameRu,
        phase: 5,
      });
      matched.add(v2.kod);
      phase5Count++;
    }
  }
  console.log(`Phase 5 matches: ${phase5Count}`);

  // ========================================
  // Special: Exact unique no geo
  // ========================================
  console.log('\n=== Special: Exact unique no geo ===');
  let specialCount = 0;

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;
    if (!v2.geoId && v2.category !== 'garden_coop') {
      // No geo info at all — try exact unique match
      const normRu = normalize(v2.nameRu);
      const normUa = normalize(v2.nameUa);

      for (const norm of [normRu, normUa]) {
        if (!norm || norm.length < 3) continue;
        const candidates = ourByNormalized.get(norm);
        if (candidates && candidates.length === 1) {
          // Unique in our DB
          results.push({
            v2Kod: v2.kod,
            localId: candidates[0].id,
            confidence: 0.70,
            method: 'exact_unique_no_geo',
            v2Name: v2.nameRu || v2.nameUa,
            ourName: candidates[0].nameUk || candidates[0].nameRu,
            phase: -1,
          });
          matched.add(v2.kod);
          specialCount++;
          break;
        }
      }
    }
  }
  console.log(`Special exact unique no-geo matches: ${specialCount}`);

  // ========================================
  // Phase 6: Create new apartment_complexes
  // ========================================
  console.log('\n=== Phase 6: Create new complexes ===');
  let phase6Count = 0;
  const newComplexes = [];

  for (const v2 of v2Complexes) {
    if (matched.has(v2.kod)) continue;
    if (v2.category !== 'zhk' && v2.category !== 'cottage') continue;

    const lookup = lookupByKod.get(v2.kod);
    if (!lookup || !lookup.lat || !lookup.lng) continue;

    // Check if there's already a complex within 200m with similar name
    let tooClose = false;
    for (const c of ourComplexes) {
      if (!c.lat || !c.lng) continue;
      const dist = approxDistanceKm(lookup.lat, lookup.lng, c.lat, c.lng);
      if (dist <= 0.2) {
        const sim = Math.max(
          similarity(normalize(v2.nameRu), c.normRu),
          similarity(normalize(v2.nameRu), c.normUk),
          similarity(normalize(v2.nameUa), c.normRu),
          similarity(normalize(v2.nameUa), c.normUk),
        );
        if (sim >= 0.50) {
          tooClose = true;
          break;
        }
      }
    }

    if (tooClose) continue;

    // Determine geo_id
    const nearestCity = findNearestCity(lookup.lat, lookup.lng);
    const geoId = nearestCity ? nearestCity.id : null;

    const nameRu = (v2.nameRu || v2.nameUa || '').replace(/^(ЖК|СК|КМ|КП)\s+/i, '').trim();
    const nameUk = (v2.nameUa || v2.nameRu || '').replace(/^(ЖК|СК|КМ|КП)\s+/i, '').trim();
    const nameNorm = normalize(nameRu || nameUk);

    newComplexes.push({
      v2Kod: v2.kod,
      nameRu,
      nameUk,
      nameNormalized: nameNorm,
      lat: lookup.lat,
      lng: lookup.lng,
      geoId,
      source: 'web_search',
    });
  }

  // Insert new complexes and create mappings
  for (const nc of newComplexes) {
    try {
      const insertResult = await client.query(
        `INSERT INTO apartment_complexes (name_ru, name_uk, name_normalized, lat, lng, geo_id, source, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
         RETURNING id`,
        [nc.nameRu, nc.nameUk, nc.nameNormalized, nc.lat, nc.lng, nc.geoId, nc.source]
      );
      const newId = insertResult.rows[0].id;

      results.push({
        v2Kod: nc.v2Kod,
        localId: newId,
        confidence: 0.75,
        method: 'new_created',
        v2Name: nc.nameRu,
        ourName: nc.nameRu,
        phase: 6,
      });
      matched.add(nc.v2Kod);
      phase6Count++;
    } catch (err) {
      console.error(`  Failed to create complex for kod=${nc.v2Kod}: ${err.message}`);
    }
  }
  console.log(`Phase 6 new complexes: ${phase6Count}`);

  // ========================================
  // Section 7: Write results
  // ========================================
  console.log('\n=== Section 7: Writing results ===');

  // Delete old mappings
  const deleteResult = await client.query(
    `DELETE FROM source_id_mappings WHERE source = $1 AND entity_type = 'complex'`,
    [SOURCE]
  );
  console.log(`Deleted ${deleteResult.rowCount} old complex mappings`);

  // Insert new mappings in batches
  const BATCH_SIZE = 500;
  let insertCount = 0;
  for (let i = 0; i < results.length; i += BATCH_SIZE) {
    const batch = results.slice(i, i + BATCH_SIZE);
    const values = [];
    const params = [];
    let paramIdx = 1;

    for (const r of batch) {
      values.push(`($${paramIdx}, 'complex', $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4})`);
      params.push(SOURCE, r.v2Kod, r.localId, r.confidence, r.method);
      paramIdx += 5;
    }

    await client.query(
      `INSERT INTO source_id_mappings (source, entity_type, source_id, local_id, confidence, match_method)
       VALUES ${values.join(',')}
       ON CONFLICT (source, entity_type, source_id) DO UPDATE
       SET local_id = EXCLUDED.local_id, confidence = EXCLUDED.confidence, match_method = EXCLUDED.match_method`,
      params
    );
    insertCount += batch.length;
  }
  console.log(`Inserted ${insertCount} new complex mappings`);

  // ========================================
  // Section 8: Report
  // ========================================
  console.log('\n========================================');
  console.log('=== FINAL REPORT ===');
  console.log('========================================');

  const byPhase = {};
  for (const r of results) {
    const key = `Phase ${r.phase}: ${r.method}`;
    byPhase[key] = (byPhase[key] || 0) + 1;
  }
  for (const [phase, count] of Object.entries(byPhase).sort()) {
    console.log(`  ${phase}: ${count}`);
  }

  const unmatched = v2Complexes.filter(v => !matched.has(v.kod));
  console.log(`\nSummary:`);
  console.log(`  Total v2 complexes: ${v2Complexes.length}`);
  console.log(`  Garden coops (skipped): ${gardenCoops.length}`);
  console.log(`  Matched: ${results.length}`);
  console.log(`  Manual review: ${manualReview.length}`);
  console.log(`  Unmatched: ${unmatched.length}`);

  // Confidence breakdown
  const byConfidence = {};
  for (const r of results) {
    byConfidence[r.confidence] = (byConfidence[r.confidence] || 0) + 1;
  }
  console.log(`\nBy confidence:`);
  for (const [conf, count] of Object.entries(byConfidence).sort((a, b) => parseFloat(b[0]) - parseFloat(a[0]))) {
    console.log(`  ${conf}: ${count}`);
  }

  // Verify: check if old conf=1.00 matches are preserved
  console.log(`\nVerification: checking old conf=1.00 matches...`);
  const oldExactResult = await client.query(
    `SELECT source_id, local_id, match_method FROM source_id_mappings
     WHERE source = $1 AND entity_type = 'complex' AND confidence = 1.00`,
    [SOURCE]
  );
  console.log(`  New conf=1.00 mappings: ${oldExactResult.rows.length}`);

  // Save reports
  const reportDir = TEMP;

  // Manual review CSV
  if (manualReview.length > 0) {
    const csvLines = ['v2_kod,v2_name,our_id,our_name,similarity,number_ok'];
    for (const mr of manualReview) {
      csvLines.push(`${mr.v2Kod},"${mr.v2Name}",${mr.ourId},"${mr.ourName}",${mr.similarity},${mr.numberOk}`);
    }
    const mrPath = path.join(reportDir, 'complex_manual_review.csv');
    fs.writeFileSync(mrPath, csvLines.join('\n'), 'utf-8');
    console.log(`\nManual review saved: ${mrPath}`);
  }

  // Unmatched CSV
  if (unmatched.length > 0) {
    const csvLines = ['kod,name_ru,name_ua,geo_id,category,has_lookup_coords'];
    for (const v2 of unmatched) {
      const hasCoords = lookupByKod.has(v2.kod) && lookupByKod.get(v2.kod).lat ? 'yes' : 'no';
      csvLines.push(`${v2.kod},"${v2.nameRu}","${v2.nameUa}",${v2.geoId || ''},${v2.category},${hasCoords}`);
    }
    const umPath = path.join(reportDir, 'complex_unmatched_final.csv');
    fs.writeFileSync(umPath, csvLines.join('\n'), 'utf-8');
    console.log(`Unmatched saved: ${umPath}`);
  }

  // Garden coops CSV
  if (gardenCoops.length > 0) {
    const csvLines = ['kod,name_ru,name_ua,geo_id'];
    for (const v2 of gardenCoops) {
      csvLines.push(`${v2.kod},"${v2.nameRu}","${v2.nameUa}",${v2.geoId || ''}`);
    }
    const gcPath = path.join(reportDir, 'complex_garden_coops.csv');
    fs.writeFileSync(gcPath, csvLines.join('\n'), 'utf-8');
    console.log(`Garden coops saved: ${gcPath}`);
  }

  // Full report JSON
  const report = {
    timestamp: new Date().toISOString(),
    summary: {
      total: v2Complexes.length,
      gardenCoops: gardenCoops.length,
      matched: results.length,
      manualReview: manualReview.length,
      unmatched: unmatched.length,
    },
    byPhase,
    byConfidence,
    results: results.map(r => ({
      v2Kod: r.v2Kod,
      localId: r.localId,
      confidence: r.confidence,
      method: r.method,
      v2Name: r.v2Name,
      ourName: r.ourName,
    })),
    manualReview,
    unmatched: unmatched.map(v => ({
      kod: v.kod,
      nameRu: v.nameRu,
      nameUa: v.nameUa,
      geoId: v.geoId,
      category: v.category,
    })),
  };
  const reportPath = path.join(reportDir, 'rematch_report.json');
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), 'utf-8');
  console.log(`Full report saved: ${reportPath}`);

  // Show some examples from each phase
  console.log('\n--- Sample matches by phase ---');
  for (let phase = 0; phase <= 6; phase++) {
    const phaseResults = results.filter(r => r.phase === phase);
    if (phaseResults.length === 0) continue;
    console.log(`\n  Phase ${phase} (${phaseResults.length} matches):`);
    for (const r of phaseResults.slice(0, 3)) {
      console.log(`    v2:${r.v2Kod} "${r.v2Name}" → our:${r.localId} "${r.ourName}" [${r.confidence}|${r.method}]`);
    }
  }
  // Special phase
  const specialResults = results.filter(r => r.phase === -1);
  if (specialResults.length > 0) {
    console.log(`\n  Special exact_unique_no_geo (${specialResults.length} matches):`);
    for (const r of specialResults.slice(0, 3)) {
      console.log(`    v2:${r.v2Kod} "${r.v2Name}" → our:${r.localId} "${r.ourName}" [${r.confidence}|${r.method}]`);
    }
  }

  // Show some unmatched
  if (unmatched.length > 0) {
    console.log(`\n--- First 20 unmatched ---`);
    for (const v2 of unmatched.slice(0, 20)) {
      const lookup = lookupByKod.get(v2.kod);
      const coords = lookup && lookup.lat ? `(${lookup.lat.toFixed(4)}, ${lookup.lng.toFixed(4)})` : 'no coords';
      console.log(`  v2:${v2.kod} "${v2.nameRu}" geo=${v2.geoId || 'null'} cat=${v2.category} ${coords}`);
    }
  }

  await client.end();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
