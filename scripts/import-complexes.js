const fs = require('fs');
const https = require('https');
const path = require('path');

// Normalize name for matching
function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/жк|жилой комплекс|житловий комплекс|residential complex/gi, '')
    .replace(/кг|км|котеджне|коттеджне|містечко|селище/gi, '')
    .replace(/таунхаус[иі]?|townhouse[s]?|дуплекс[иі]?|duplex[es]?/gi, '')
    .replace(/["«»'']/g, '')
    .replace(/[^\wа-яіїєґ\s]/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Parse GeoVector.csv
function parseGeoVectorCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n').slice(1).filter(l => l.trim());

  return lines.map((line, idx) => {
    const parts = line.match(/(?:"[^"]*"|[^,])+/g) || [];
    const ru = (parts[0] || '').replace(/"/g, '').trim();
    const uk = (parts[1] || '').replace(/"/g, '').trim();
    const en = (parts[2] || '').replace(/"/g, '').trim();
    const lat = parseFloat(parts[3]);
    const lng = parseFloat(parts[4]);

    return {
      id: idx + 1,
      nameRu: ru,
      nameUk: uk || ru,
      nameEn: en,
      lat,
      lng,
      source: 'geovector',
    };
  }).filter(c => c.lat && c.lng && !isNaN(c.lat) && !isNaN(c.lng));
}

// Query Overpass API
async function queryOverpassAPI(bbox) {
  const query = `
[out:json][timeout:120];
(
  way["building"]["name"](${bbox});
  way["landuse"="residential"]["name"](${bbox});
  relation["building"]["name"](${bbox});
  relation["landuse"="residential"]["name"](${bbox});
);
out body geom;
`;

  return new Promise((resolve, reject) => {
    const postData = 'data=' + encodeURIComponent(query);

    const options = {
      hostname: 'overpass-api.de',
      path: '/api/interpreter',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    console.log('Querying Overpass API...');

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          console.log(`Found ${json.elements?.length || 0} elements in OSM`);
          resolve(json.elements || []);
        } catch (e) {
          reject(new Error('Failed to parse OSM response: ' + e.message));
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

// Calculate similarity
function similarity(s1, s2) {
  const n1 = normalizeName(s1);
  const n2 = normalizeName(s2);

  if (n1 === n2) return 1;
  if (n1.includes(n2) || n2.includes(n1)) return 0.9;

  const words1 = new Set(n1.split(' ').filter(w => w.length > 2));
  const words2 = new Set(n2.split(' ').filter(w => w.length > 2));

  if (words1.size === 0 || words2.size === 0) return 0;

  const intersection = [...words1].filter(w => words2.has(w)).length;
  const union = new Set([...words1, ...words2]).size;

  return intersection / union;
}

// Calculate distance in meters
function haversineDistance(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLng/2) * Math.sin(dLng/2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}

// Match complexes
function matchComplexes(geoVector, osmElements) {
  const result = [];
  const matchedOsmIds = new Set();

  const osmComplexes = osmElements
    .filter(e => e.tags?.name)
    .map(e => {
      let lat, lng;
      let polygon;

      if (e.geometry && e.geometry.length > 0) {
        const sumLat = e.geometry.reduce((s, p) => s + p.lat, 0);
        const sumLng = e.geometry.reduce((s, p) => s + p.lon, 0);
        lat = sumLat / e.geometry.length;
        lng = sumLng / e.geometry.length;
        polygon = e.geometry.map(p => [p.lon, p.lat]);
      } else if (e.lat && e.lon) {
        lat = e.lat;
        lng = e.lon;
      } else {
        return null;
      }

      return {
        osmId: e.id,
        osmType: e.type,
        nameRu: e.tags.name,
        nameUk: e.tags['name:uk'] || e.tags.name,
        nameEn: e.tags['name:en'],
        lat,
        lng,
        polygon,
        source: 'osm',
      };
    })
    .filter(c => c !== null);

  console.log(`Processed ${osmComplexes.length} OSM complexes with names`);

  // Match GeoVector with OSM
  for (const gv of geoVector) {
    let bestMatch = null;
    let bestScore = 0;

    for (const osm of osmComplexes) {
      if (matchedOsmIds.has(osm.osmId)) continue;

      const dist = haversineDistance(gv.lat, gv.lng, osm.lat, osm.lng);
      if (dist > 500) continue;

      const nameSim = Math.max(
        similarity(gv.nameRu, osm.nameRu),
        similarity(gv.nameUk, osm.nameUk),
        gv.nameEn && osm.nameEn ? similarity(gv.nameEn, osm.nameEn) : 0,
      );

      const distFactor = 1 - (dist / 500) * 0.3;
      const score = nameSim * distFactor;

      if (score > bestScore && score > 0.5) {
        bestScore = score;
        bestMatch = osm;
      }
    }

    if (bestMatch) {
      matchedOsmIds.add(bestMatch.osmId);
      result.push({
        id: gv.id,
        osmId: bestMatch.osmId,
        osmType: bestMatch.osmType,
        nameRu: gv.nameRu,
        nameUk: gv.nameUk,
        nameEn: gv.nameEn,
        lat: bestMatch.lat,
        lng: bestMatch.lng,
        polygon: bestMatch.polygon,
        source: 'merged',
      });
      console.log(`  Matched: "${gv.nameRu}" → OSM "${bestMatch.nameRu}" (score: ${bestScore.toFixed(2)})`);
    } else {
      result.push(gv);
    }
  }

  // Add unmatched OSM complexes
  for (const osm of osmComplexes) {
    if (!matchedOsmIds.has(osm.osmId)) {
      result.push({
        ...osm,
        id: result.length + 1,
      });
    }
  }

  return result;
}

// Generate SQL
function generateSQL(complexes) {
  const lines = [];

  lines.push(`-- Apartment complexes import
-- Generated: ${new Date().toISOString()}
-- Total: ${complexes.length} complexes

-- Enable trigram extension for fuzzy search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP TABLE IF EXISTS apartment_complexes CASCADE;

CREATE TABLE apartment_complexes (
  id SERIAL PRIMARY KEY,
  osm_id BIGINT,
  osm_type VARCHAR(20),
  name_ru VARCHAR(255) NOT NULL,
  name_uk VARCHAR(255) NOT NULL,
  name_en VARCHAR(255),
  name_normalized VARCHAR(255) NOT NULL,
  lat DECIMAL(10, 8) NOT NULL,
  lng DECIMAL(11, 8) NOT NULL,
  polygon GEOMETRY(POLYGON, 4326),
  geo_id INTEGER,
  street_id INTEGER,
  topzone_id INTEGER,
  source VARCHAR(20) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ac_name_normalized ON apartment_complexes(name_normalized);
CREATE INDEX idx_ac_name_ru_trgm ON apartment_complexes USING gin(name_ru gin_trgm_ops);
CREATE INDEX idx_ac_name_uk_trgm ON apartment_complexes USING gin(name_uk gin_trgm_ops);
CREATE INDEX idx_ac_coords ON apartment_complexes(lat, lng);
CREATE INDEX idx_ac_geo ON apartment_complexes(geo_id);
CREATE INDEX idx_ac_polygon ON apartment_complexes USING GIST(polygon);

`);

  for (const c of complexes) {
    const nameNormalized = normalizeName(c.nameRu || c.nameUk);
    let polygonWKT = 'NULL';

    if (c.polygon && c.polygon.length >= 3) {
      const coords = c.polygon.map(p => `${p[0]} ${p[1]}`).join(', ');
      const firstCoord = `${c.polygon[0][0]} ${c.polygon[0][1]}`;
      polygonWKT = `ST_GeomFromText('POLYGON((${coords}, ${firstCoord}))', 4326)`;
    }

    const esc = (s) => s ? `'${s.replace(/'/g, "''")}'` : 'NULL';

    lines.push(`INSERT INTO apartment_complexes (osm_id, osm_type, name_ru, name_uk, name_en, name_normalized, lat, lng, polygon, source) VALUES (
  ${c.osmId || 'NULL'}, ${c.osmType ? `'${c.osmType}'` : 'NULL'}, ${esc(c.nameRu)}, ${esc(c.nameUk)}, ${esc(c.nameEn)}, ${esc(nameNormalized)}, ${c.lat}, ${c.lng}, ${polygonWKT}, '${c.source}'
);`);
  }

  lines.push(`
-- Stats
SELECT source, COUNT(*) as count, COUNT(polygon) as with_polygon FROM apartment_complexes GROUP BY source;
SELECT COUNT(*) as total FROM apartment_complexes;
`);

  return lines.join('\n');
}

// Main
async function main() {
  const geoVectorPath = 'D:/analogis/GeoVector.csv';
  const outputDir = 'D:/analogis/liquidity-define/db';

  console.log('=== Apartment Complexes Import ===\\n');

  // 1. Parse GeoVector.csv
  console.log('1. Parsing GeoVector.csv...');
  const geoVector = parseGeoVectorCSV(geoVectorPath);
  console.log(`   Found ${geoVector.length} complexes\\n`);

  // Filter to Odessa region
  const odessaGV = geoVector.filter(c =>
    c.lat >= 46.3 && c.lat <= 46.8 && c.lng >= 30.4 && c.lng <= 31.1
  );
  console.log(`   Odessa region: ${odessaGV.length} complexes\\n`);

  // 2. Query Overpass API
  console.log('2. Querying Overpass API for OSM data...');
  const osmElements = await queryOverpassAPI('46.3,30.4,46.8,31.1');

  // 3. Match and merge
  console.log('\\n3. Matching GeoVector with OSM...');
  const merged = matchComplexes(odessaGV, osmElements);

  const stats = {
    total: merged.length,
    withPolygon: merged.filter(c => c.polygon).length,
    fromGeoVector: merged.filter(c => c.source === 'geovector').length,
    fromOSM: merged.filter(c => c.source === 'osm').length,
    merged: merged.filter(c => c.source === 'merged').length,
  };

  console.log('\\n4. Statistics:');
  console.log(`   Total complexes: ${stats.total}`);
  console.log(`   With polygon: ${stats.withPolygon}`);
  console.log(`   From GeoVector only: ${stats.fromGeoVector}`);
  console.log(`   From OSM only: ${stats.fromOSM}`);
  console.log(`   Merged (matched): ${stats.merged}`);

  // 4. Generate SQL
  console.log('\\n5. Generating SQL...');
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const sqlPath = path.join(outputDir, 'apartment_complexes.sql');
  const sql = generateSQL(merged);
  fs.writeFileSync(sqlPath, sql);
  console.log(`   SQL saved to: ${sqlPath}`);

  const jsonPath = path.join(outputDir, 'apartment_complexes.json');
  fs.writeFileSync(jsonPath, JSON.stringify(merged, null, 2));
  console.log(`   JSON saved to: ${jsonPath}`);

  console.log('\\n=== Done ===');
}

main().catch(console.error);
