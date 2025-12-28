/**
 * Import polygons from OSM Overpass API for existing apartment complexes
 *
 * This script:
 * 1. Fetches all apartment complexes from the database
 * 2. Queries OSM Overpass API for building polygons in Odessa region
 * 3. Updates the polygon column for matching complexes
 */

import { DataSource } from 'typeorm';
import * as https from 'https';

// Load .env file
import * as dotenv from 'dotenv';
import * as path from 'path';
dotenv.config({ path: path.resolve(__dirname, '../.env') });

// Database connection
const dataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
});

interface OsmElement {
  type: string;
  id: number;
  tags?: Record<string, string>;
  geometry?: { lat: number; lon: number }[];
  lat?: number;
  lon?: number;
  bounds?: { minlat: number; minlon: number; maxlat: number; maxlon: number };
}

interface Complex {
  id: number;
  osm_id: number | null;
  osm_type: string | null;
  name_ru: string;
  name_uk: string;
  lat: number;
  lng: number;
  has_polygon: boolean;
}

// Query Overpass API with retry on different servers
const OVERPASS_SERVERS = [
  'overpass-api.de',
  'lz4.overpass-api.de',
  'z.overpass-api.de',
];

async function queryOverpassAPI(query: string, serverIndex = 0): Promise<OsmElement[]> {
  if (serverIndex >= OVERPASS_SERVERS.length) {
    throw new Error('All Overpass servers failed');
  }

  const server = OVERPASS_SERVERS[serverIndex];

  return new Promise((resolve, reject) => {
    const postData = 'data=' + encodeURIComponent(query);

    const options = {
      hostname: server,
      path: '/api/interpreter',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
        'User-Agent': 'ValuationService/1.0',
      },
    };

    console.log(`   Trying server: ${server}...`);

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        // Check for error status
        if (res.statusCode !== 200) {
          console.log(`   Server ${server} returned status ${res.statusCode}, trying next...`);
          queryOverpassAPI(query, serverIndex + 1).then(resolve).catch(reject);
          return;
        }

        try {
          const json = JSON.parse(data);
          resolve(json.elements || []);
        } catch (e) {
          // Check if it's an error response
          if (data.includes('<html>') || data.includes('<?xml')) {
            console.log(`   Server ${server} returned error page, trying next...`);
            queryOverpassAPI(query, serverIndex + 1).then(resolve).catch(reject);
            return;
          }
          reject(new Error('Failed to parse OSM response: ' + (e as Error).message));
        }
      });
    });

    req.on('error', (err) => {
      console.log(`   Server ${server} error: ${err.message}, trying next...`);
      queryOverpassAPI(query, serverIndex + 1).then(resolve).catch(reject);
    });

    req.setTimeout(120000, () => {
      req.destroy();
      console.log(`   Server ${server} timeout, trying next...`);
      queryOverpassAPI(query, serverIndex + 1).then(resolve).catch(reject);
    });

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
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLng/2) * Math.sin(dLng/2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}

// Convert OSM geometry to WKT Polygon
function geometryToWKT(geometry: { lat: number; lon: number }[]): string | null {
  if (!geometry || geometry.length < 3) return null;

  // Close the polygon if not closed
  const points = [...geometry];
  if (points[0].lat !== points[points.length - 1].lat ||
      points[0].lon !== points[points.length - 1].lon) {
    points.push(points[0]);
  }

  const coords = points.map(p => `${p.lon} ${p.lat}`).join(', ');
  return `POLYGON((${coords}))`;
}

// Calculate centroid from geometry
function getCentroid(geometry: { lat: number; lon: number }[]): { lat: number; lng: number } {
  const sumLat = geometry.reduce((s, p) => s + p.lat, 0);
  const sumLng = geometry.reduce((s, p) => s + p.lon, 0);
  return {
    lat: sumLat / geometry.length,
    lng: sumLng / geometry.length,
  };
}

async function main() {
  console.log('=== Import Polygons from OSM ===\n');

  // Connect to database
  await dataSource.initialize();
  console.log('Connected to database\n');

  try {
    // 0. Ensure polygon column exists
    console.log('0. Ensuring polygon column exists...');
    try {
      await dataSource.query(`
        ALTER TABLE apartment_complexes
        ADD COLUMN IF NOT EXISTS polygon geometry(Polygon, 4326)
      `);
      console.log('   Polygon column ready\n');
    } catch (err) {
      // Column might already exist or syntax differs
      console.log('   Note: ' + (err as Error).message + '\n');
    }

    // 1. Get all apartment complexes
    console.log('1. Fetching existing complexes...');
    const complexes: Complex[] = await dataSource.query(`
      SELECT id, osm_id, osm_type, name_ru, name_uk, lat::float, lng::float,
             (polygon IS NOT NULL) as has_polygon
      FROM apartment_complexes
      ORDER BY id
    `);
    console.log(`   Found ${complexes.length} complexes`);
    console.log(`   Already have polygons: ${complexes.filter(c => c.has_polygon).length}`);
    console.log(`   With OSM ID: ${complexes.filter(c => c.osm_id).length}\n`);

    // 2. Query OSM for buildings with geometry
    console.log('2. Querying Overpass API for buildings in Odessa...');
    const query = `
[out:json][timeout:180];
(
  way["building"]["name"](46.3,30.4,46.8,31.1);
  way["landuse"="residential"]["name"](46.3,30.4,46.8,31.1);
  relation["building"]["name"](46.3,30.4,46.8,31.1);
  relation["landuse"="residential"]["name"](46.3,30.4,46.8,31.1);
);
out body geom;
`;

    const osmElements = await queryOverpassAPI(query);
    console.log(`   Found ${osmElements.length} OSM elements with names\n`);

    // Build OSM lookup by ID
    const osmById = new Map<string, OsmElement>();
    for (const el of osmElements) {
      const key = `${el.type}/${el.id}`;
      osmById.set(key, el);
    }

    // 3. Match complexes by OSM ID first
    console.log('3. Matching by OSM ID...');
    let matchedById = 0;
    let matchedByName = 0;
    let updated = 0;
    let failed = 0;

    for (const complex of complexes) {
      if (complex.has_polygon) continue; // Skip if already has polygon

      let osmElement: OsmElement | undefined;

      // Try matching by OSM ID
      if (complex.osm_id && complex.osm_type) {
        const key = `${complex.osm_type}/${complex.osm_id}`;
        osmElement = osmById.get(key);
        if (osmElement) {
          matchedById++;
        }
      }

      // If no match by ID, try matching by name and proximity
      if (!osmElement) {
        let bestMatch: OsmElement | undefined;
        let bestScore = 0;

        for (const el of osmElements) {
          if (!el.geometry || !el.tags?.name) continue;

          const centroid = getCentroid(el.geometry);
          const dist = haversineDistance(complex.lat, complex.lng, centroid.lat, centroid.lng);

          // Must be within 300m
          if (dist > 300) continue;

          const nameSim = Math.max(
            similarity(complex.name_ru, el.tags.name),
            el.tags['name:uk'] ? similarity(complex.name_uk, el.tags['name:uk']) : 0,
          );

          // Score: name similarity with distance penalty
          const score = nameSim * (1 - dist / 300 * 0.3);

          if (score > bestScore && score > 0.6) {
            bestScore = score;
            bestMatch = el;
          }
        }

        if (bestMatch) {
          osmElement = bestMatch;
          matchedByName++;
        }
      }

      // Update polygon if we found a match
      if (osmElement && osmElement.geometry) {
        const wkt = geometryToWKT(osmElement.geometry);
        if (wkt) {
          try {
            await dataSource.query(`
              UPDATE apartment_complexes
              SET polygon = ST_GeomFromText($1, 4326),
                  osm_id = $2,
                  osm_type = $3,
                  updated_at = NOW()
              WHERE id = $4
            `, [wkt, osmElement.id, osmElement.type, complex.id]);
            updated++;
            console.log(`   ✓ Updated: ${complex.name_ru} (${osmElement.type}/${osmElement.id})`);
          } catch (err) {
            failed++;
            console.log(`   ✗ Failed to update ${complex.name_ru}: ${(err as Error).message}`);
          }
        }
      }
    }

    // 4. Summary
    console.log('\n4. Summary:');
    console.log(`   Matched by OSM ID: ${matchedById}`);
    console.log(`   Matched by name: ${matchedByName}`);
    console.log(`   Updated polygons: ${updated}`);
    console.log(`   Failed updates: ${failed}`);

    // Final stats
    const finalStats = await dataSource.query(`
      SELECT
        COUNT(*) as total,
        COUNT(polygon) as with_polygon,
        COUNT(osm_id) as with_osm_id
      FROM apartment_complexes
    `);
    console.log('\n5. Final database state:');
    console.log(`   Total complexes: ${finalStats[0].total}`);
    console.log(`   With polygon: ${finalStats[0].with_polygon}`);
    console.log(`   With OSM ID: ${finalStats[0].with_osm_id}`);

  } catch (error) {
    console.error('Error:', error);
  } finally {
    await dataSource.destroy();
  }

  console.log('\n=== Done ===');
}

main().catch(console.error);
