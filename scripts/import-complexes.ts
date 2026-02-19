import * as fs from 'fs';
import * as https from 'https';
import * as path from 'path';

// Overpass API configuration
const OVERPASS_HOST = process.env.OVERPASS_HOST || 'overpass.atlanta.ua';
const OVERPASS_PATH = process.env.OVERPASS_PATH || '/api/interpreter';
const OVERPASS_AUTH = process.env.OVERPASS_AUTH || 'admin:kiTO28hgiADPRBEN';
// Delay between regions in ms (0 for local/private API, 5000 for public)
const OVERPASS_DELAY = parseInt(process.env.OVERPASS_DELAY || '500', 10);

interface Complex {
  id?: number;
  osmId?: number;
  osmType?: string;
  nameRu: string;
  nameUk: string;
  nameEn?: string;
  lat: number;
  lng: number;
  polygon?: number[][]; // [[lng, lat], ...]
  geoId?: number;
  streetId?: number;
  source: 'geovector' | 'osm' | 'merged';
  region?: string;
}

interface OsmElement {
  type: string;
  id: number;
  lat?: number;
  lon?: number;
  nodes?: number[];
  tags?: Record<string, string>;
  geometry?: { lat: number; lon: number }[];
}

// All supported regions with bounding boxes
const REGIONS: Record<string, { bbox: string; latRange: [number, number]; lngRange: [number, number] }> = {
  odesa: {
    bbox: '46.3,30.4,46.8,31.1',
    latRange: [46.3, 46.8],
    lngRange: [30.4, 31.1],
  },
  kyiv: {
    bbox: '50.2,30.2,50.65,31.0',
    latRange: [50.2, 50.65],
    lngRange: [30.2, 31.0],
  },
  kharkiv: {
    bbox: '49.85,36.05,50.15,36.45',
    latRange: [49.85, 50.15],
    lngRange: [36.05, 36.45],
  },
  dnipro: {
    bbox: '48.35,34.85,48.55,35.2',
    latRange: [48.35, 48.55],
    lngRange: [34.85, 35.2],
  },
  lviv: {
    bbox: '49.75,23.85,49.95,24.15',
    latRange: [49.75, 49.95],
    lngRange: [23.85, 24.15],
  },
  zaporizhzhia: {
    bbox: '47.75,35.0,47.95,35.25',
    latRange: [47.75, 47.95],
    lngRange: [35.0, 35.25],
  },
  kryvyi_rih: {
    bbox: '47.85,33.25,48.05,33.55',
    latRange: [47.85, 48.05],
    lngRange: [33.25, 33.55],
  },
  mykolaiv: {
    bbox: '46.9,31.9,47.05,32.1',
    latRange: [46.9, 47.05],
    lngRange: [31.9, 32.1],
  },
};

// Parse GeoVector.csv
function parseGeoVectorCSV(filePath: string): Complex[] {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n').slice(1).filter(l => l.trim());

  return lines.map((line, idx) => {
    // Handle quoted fields
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
      source: 'geovector' as const,
    };
  }).filter(c => c.lat && c.lng && !isNaN(c.lat) && !isNaN(c.lng));
}

// Filter complexes by region bounding box
function filterByRegion(complexes: Complex[], region: string): Complex[] {
  const config = REGIONS[region];
  if (!config) return [];

  return complexes.filter(c =>
    c.lat >= config.latRange[0] && c.lat <= config.latRange[1] &&
    c.lng >= config.lngRange[0] && c.lng <= config.lngRange[1]
  ).map(c => ({ ...c, region }));
}

// Query Overpass API for residential complexes with geometry
async function queryOverpassAPI(bbox: string): Promise<OsmElement[]> {
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
      hostname: OVERPASS_HOST,
      path: OVERPASS_PATH,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
        'Authorization': 'Basic ' + Buffer.from(OVERPASS_AUTH).toString('base64'),
      },
    };

    console.log(`  Querying Overpass API (${OVERPASS_HOST})...`);

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          console.log(`  Found ${json.elements?.length || 0} elements in OSM`);
          resolve(json.elements || []);
        } catch (e: any) {
          reject(new Error('Failed to parse OSM response: ' + e.message));
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

// Normalize name for matching
function normalizeName(name: string): string {
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

// Calculate similarity between two strings
function similarity(s1: string, s2: string): number {
  const n1 = normalizeName(s1);
  const n2 = normalizeName(s2);

  if (n1 === n2) return 1;
  if (n1.includes(n2) || n2.includes(n1)) return 0.9;

  // Jaccard similarity on words
  const words1 = new Set(n1.split(' ').filter(w => w.length > 2));
  const words2 = new Set(n2.split(' ').filter(w => w.length > 2));

  if (words1.size === 0 || words2.size === 0) return 0;

  const intersection = [...words1].filter(w => words2.has(w)).length;
  const union = new Set([...words1, ...words2]).size;

  return intersection / union;
}

// Calculate distance between two points in meters
function haversineDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371000; // Earth radius in meters
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLng/2) * Math.sin(dLng/2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}

// Match GeoVector complexes with OSM data
function matchComplexes(geoVector: Complex[], osmElements: OsmElement[], region: string): Complex[] {
  const result: Complex[] = [];
  const matchedOsmIds = new Set<number>();

  // Process OSM elements
  const osmComplexes: Complex[] = osmElements
    .filter(e => e.tags?.name)
    .map(e => {
      let lat: number, lng: number;
      let polygon: number[][] | undefined;

      if (e.geometry && e.geometry.length > 0) {
        // Calculate centroid
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
        nameRu: e.tags!.name,
        nameUk: e.tags!['name:uk'] || e.tags!.name,
        nameEn: e.tags!['name:en'],
        lat,
        lng,
        polygon,
        source: 'osm' as const,
        region,
      } as Complex;
    })
    .filter((c): c is Complex => c !== null);

  console.log(`  Processed ${osmComplexes.length} OSM complexes with names`);

  // Match GeoVector with OSM
  for (const gv of geoVector) {
    let bestMatch: Complex | null = null;
    let bestScore = 0;

    for (const osm of osmComplexes) {
      if (matchedOsmIds.has(osm.osmId!)) continue;

      // Check distance (must be within 500m)
      const dist = haversineDistance(gv.lat, gv.lng, osm.lat, osm.lng);
      if (dist > 500) continue;

      // Check name similarity
      const nameSim = Math.max(
        similarity(gv.nameRu, osm.nameRu),
        similarity(gv.nameUk, osm.nameUk),
        gv.nameEn && osm.nameEn ? similarity(gv.nameEn, osm.nameEn) : 0,
      );

      // Combined score: name similarity * distance factor
      const distFactor = 1 - (dist / 500) * 0.3; // Max 30% penalty for distance
      const score = nameSim * distFactor;

      if (score > bestScore && score > 0.5) {
        bestScore = score;
        bestMatch = osm;
      }
    }

    if (bestMatch) {
      // Merge: use OSM polygon, GeoVector names
      matchedOsmIds.add(bestMatch.osmId!);
      result.push({
        id: gv.id,
        osmId: bestMatch.osmId,
        osmType: bestMatch.osmType,
        nameRu: gv.nameRu,
        nameUk: gv.nameUk,
        nameEn: gv.nameEn,
        lat: bestMatch.lat, // Use OSM centroid
        lng: bestMatch.lng,
        polygon: bestMatch.polygon,
        source: 'merged',
        region,
      });
      console.log(`    Matched: "${gv.nameRu}" → OSM "${bestMatch.nameRu}" (score: ${bestScore.toFixed(2)})`);
    } else {
      // No match - use GeoVector data without polygon
      result.push({ ...gv, region });
    }
  }

  // Add OSM complexes that weren't matched
  for (const osm of osmComplexes) {
    if (!matchedOsmIds.has(osm.osmId!)) {
      result.push({
        ...osm,
        id: result.length + 1,
      });
    }
  }

  return result;
}

// Generate SQL for database import (append mode - no DROP/CREATE)
function generateInsertSQL(complexes: Complex[]): string {
  const lines: string[] = [];

  for (const c of complexes) {
    const nameNormalized = normalizeName(c.nameRu || c.nameUk);
    const polygonWKT = c.polygon && c.polygon.length >= 3
      ? `ST_GeomFromText('POLYGON((${c.polygon.map(p => `${p[0]} ${p[1]}`).join(', ')}, ${c.polygon[0][0]} ${c.polygon[0][1]}))', 4326)`
      : 'NULL';

    const escapeSql = (s: string | undefined) => s ? `'${s.replace(/'/g, "''")}'` : 'NULL';

    lines.push(`INSERT INTO apartment_complexes (osm_id, osm_type, name_ru, name_uk, name_en, name_normalized, lat, lng, polygon, source) VALUES (
  ${c.osmId || 'NULL'},
  ${c.osmType ? `'${c.osmType}'` : 'NULL'},
  ${escapeSql(c.nameRu)},
  ${escapeSql(c.nameUk)},
  ${escapeSql(c.nameEn)},
  ${escapeSql(nameNormalized)},
  ${c.lat},
  ${c.lng},
  ${polygonWKT},
  '${c.source}'
);`);
  }

  return lines.join('\n');
}

// Generate full SQL with table creation
function generateFullSQL(complexes: Complex[]): string {
  const header = `-- Apartment complexes import
-- Generated: ${new Date().toISOString()}
-- Total: ${complexes.length} complexes
-- Regions: ${Object.keys(REGIONS).join(', ')}

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

CREATE INDEX idx_apartment_complexes_name_normalized ON apartment_complexes(name_normalized);
CREATE INDEX idx_apartment_complexes_name_ru ON apartment_complexes USING gin(name_ru gin_trgm_ops);
CREATE INDEX idx_apartment_complexes_name_uk ON apartment_complexes USING gin(name_uk gin_trgm_ops);
CREATE INDEX idx_apartment_complexes_coords ON apartment_complexes(lat, lng);
CREATE INDEX idx_apartment_complexes_geo ON apartment_complexes(geo_id);
CREATE INDEX idx_apartment_complexes_polygon ON apartment_complexes USING GIST(polygon);

`;

  const inserts = generateInsertSQL(complexes);

  const footer = `

-- Update geo_id based on coordinates
UPDATE apartment_complexes ac
SET geo_id = g.id
FROM geo g
WHERE ST_Contains(g.geometry, ST_SetSRID(ST_MakePoint(ac.lng, ac.lat), 4326))
  AND g.geometry IS NOT NULL;

-- Stats
SELECT
  COUNT(*) as total,
  COUNT(polygon) as with_polygon,
  COUNT(geo_id) as with_geo,
  source,
  COUNT(*)
FROM apartment_complexes
GROUP BY source;
`;

  return header + inserts + footer;
}

// Process a single region
async function processRegion(geoVector: Complex[], region: string): Promise<Complex[]> {
  console.log(`\n=== Processing region: ${region.toUpperCase()} ===`);

  const config = REGIONS[region];
  if (!config) {
    console.log(`  Unknown region: ${region}`);
    return [];
  }

  // Filter GeoVector by region
  const regionGV = filterByRegion(geoVector, region);
  console.log(`  GeoVector complexes: ${regionGV.length}`);

  // Query OSM
  const osmElements = await queryOverpassAPI(config.bbox);

  // Match and merge
  const merged = matchComplexes(regionGV, osmElements, region);

  const stats = {
    total: merged.length,
    withPolygon: merged.filter(c => c.polygon).length,
    fromGeoVector: merged.filter(c => c.source === 'geovector').length,
    fromOSM: merged.filter(c => c.source === 'osm').length,
    merged: merged.filter(c => c.source === 'merged').length,
  };

  console.log(`  Results for ${region}:`);
  console.log(`    Total: ${stats.total}`);
  console.log(`    With polygon: ${stats.withPolygon}`);
  console.log(`    GeoVector only: ${stats.fromGeoVector}`);
  console.log(`    OSM only: ${stats.fromOSM}`);
  console.log(`    Merged: ${stats.merged}`);

  return merged;
}

// Sleep helper for rate limiting
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Main function
async function main() {
  const args = process.argv.slice(2);
  const selectedRegion = args[0]?.toLowerCase();

  const geoVectorPath = path.resolve(__dirname, '../../GeoVector.csv');
  const outputPath = path.resolve(__dirname, '../db/apartment_complexes.sql');

  console.log('=== Apartment Complexes Import ===');
  console.log(`Available regions: ${Object.keys(REGIONS).join(', ')}`);

  // 1. Parse GeoVector.csv
  console.log('\n1. Parsing GeoVector.csv...');
  let geoVector: Complex[] = [];
  if (fs.existsSync(geoVectorPath)) {
    geoVector = parseGeoVectorCSV(geoVectorPath);
    console.log(`   Found ${geoVector.length} complexes in GeoVector`);
  } else {
    console.log(`   GeoVector.csv not found at ${geoVectorPath}`);
    console.log('   Will use OSM data only');
  }

  // 2. Process regions
  const allComplexes: Complex[] = [];

  if (selectedRegion && selectedRegion !== 'all') {
    // Process single region
    if (!REGIONS[selectedRegion]) {
      console.error(`Unknown region: ${selectedRegion}`);
      console.error(`Available: ${Object.keys(REGIONS).join(', ')}`);
      process.exit(1);
    }
    const result = await processRegion(geoVector, selectedRegion);
    allComplexes.push(...result);
  } else {
    // Process all regions
    console.log('\n2. Processing all regions...');
    for (const region of Object.keys(REGIONS)) {
      const result = await processRegion(geoVector, region);
      allComplexes.push(...result);

      // Rate limit for Overpass API
      if (OVERPASS_DELAY > 0 && region !== Object.keys(REGIONS).slice(-1)[0]) {
        console.log(`  Waiting ${OVERPASS_DELAY}ms before next region...`);
        await sleep(OVERPASS_DELAY);
      }
    }
  }

  // Deduplicate by osmId if present
  const seenOsmIds = new Set<number>();
  const dedupedComplexes = allComplexes.filter(c => {
    if (c.osmId) {
      if (seenOsmIds.has(c.osmId)) return false;
      seenOsmIds.add(c.osmId);
    }
    return true;
  });

  // Assign sequential IDs
  dedupedComplexes.forEach((c, idx) => {
    c.id = idx + 1;
  });

  // 3. Generate output
  console.log('\n3. Final statistics:');
  console.log(`   Total unique complexes: ${dedupedComplexes.length}`);
  console.log(`   With polygon: ${dedupedComplexes.filter(c => c.polygon).length}`);
  console.log(`   By source:`);
  console.log(`     - geovector: ${dedupedComplexes.filter(c => c.source === 'geovector').length}`);
  console.log(`     - osm: ${dedupedComplexes.filter(c => c.source === 'osm').length}`);
  console.log(`     - merged: ${dedupedComplexes.filter(c => c.source === 'merged').length}`);

  // Generate SQL
  console.log('\n4. Generating SQL...');
  const sql = generateFullSQL(dedupedComplexes);
  fs.writeFileSync(outputPath, sql);
  console.log(`   Saved to: ${outputPath}`);

  // Also save JSON for reference
  const jsonPath = path.resolve(__dirname, '../db/apartment_complexes.json');
  fs.writeFileSync(jsonPath, JSON.stringify(dedupedComplexes, null, 2));
  console.log(`   JSON saved to: ${jsonPath}`);

  console.log('\n=== Done ===');
}

main().catch(console.error);
