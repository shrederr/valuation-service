const { DataSource } = require('typeorm');
const https = require('https');
require('dotenv').config();

const ds = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
});

// Normalize for matching
function normalize(name) {
  return (name || '')
    .toLowerCase()
    .replace(/жк|ж\.к\.|житловий комплекс|жилой комплекс|residential|complex/gi, '')
    .replace(/["«»''№#\-_]/g, ' ')
    .replace(/[^\wа-яіїєґ0-9\s]/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Calculate similarity
function similarity(s1, s2) {
  const n1 = normalize(s1);
  const n2 = normalize(s2);

  if (!n1 || !n2) return 0;
  if (n1 === n2) return 1;
  if (n1.includes(n2) || n2.includes(n1)) return 0.9;

  // Word-based Jaccard
  const words1 = new Set(n1.split(' ').filter(w => w.length > 2));
  const words2 = new Set(n2.split(' ').filter(w => w.length > 2));
  if (words1.size === 0 || words2.size === 0) return 0;

  const intersection = [...words1].filter(w => words2.has(w)).length;
  const union = new Set([...words1, ...words2]).size;

  return intersection / union;
}

// Haversine distance in meters
function distance(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng/2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// Query Overpass API
async function queryOverpass(query) {
  return new Promise((resolve, reject) => {
    const postData = 'data=' + encodeURIComponent(query);
    const req = https.request({
      hostname: 'overpass-api.de',
      path: '/api/interpreter',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data).elements || []);
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(120000);
    req.write(postData);
    req.end();
  });
}

async function main() {
  await ds.initialize();
  console.log('=== Find Polygons (Fuzzy Matching) ===\n');

  // Get complexes without polygons
  const complexes = await ds.query(`
    SELECT id, name_ru, name_uk, lat::float, lng::float
    FROM apartment_complexes
    WHERE polygon IS NULL
    ORDER BY id
  `);
  console.log('Complexes without polygons:', complexes.length);

  // Query OSM for all buildings with names in Odessa region
  console.log('\nQuerying Overpass API...');
  const osmElements = await queryOverpass(`
[out:json][timeout:180];
(
  way["building"]["name"](46.3,30.4,46.8,31.1);
  way["landuse"="residential"]["name"](46.3,30.4,46.8,31.1);
  relation["building"]["name"](46.3,30.4,46.8,31.1);
);
out body geom;
`);
  console.log('OSM elements found:', osmElements.length);

  // Process OSM elements
  const osmBuildings = osmElements
    .filter(e => e.tags?.name && e.geometry?.length >= 3)
    .map(e => {
      const sumLat = e.geometry.reduce((s, p) => s + p.lat, 0);
      const sumLng = e.geometry.reduce((s, p) => s + p.lon, 0);
      return {
        id: e.id,
        type: e.type,
        name: e.tags.name,
        nameUk: e.tags['name:uk'],
        lat: sumLat / e.geometry.length,
        lng: sumLng / e.geometry.length,
        geometry: e.geometry,
      };
    });
  console.log('OSM buildings with geometry:', osmBuildings.length);

  // Match complexes to OSM buildings
  console.log('\n=== Matching ===\n');
  let matched = 0;
  let updated = 0;

  for (const complex of complexes) {
    let bestMatch = null;
    let bestScore = 0;

    for (const osm of osmBuildings) {
      // Check distance first (within 500m)
      const dist = distance(complex.lat, complex.lng, osm.lat, osm.lng);
      if (dist > 500) continue;

      // Calculate name similarity
      const sim1 = similarity(complex.name_ru, osm.name);
      const sim2 = similarity(complex.name_uk, osm.name);
      const sim3 = osm.nameUk ? similarity(complex.name_uk, osm.nameUk) : 0;
      const nameSim = Math.max(sim1, sim2, sim3);

      // Combined score: name similarity with distance bonus
      const distFactor = 1 - (dist / 500) * 0.2;
      const score = nameSim * distFactor;

      if (score > bestScore && score > 0.5) {
        bestScore = score;
        bestMatch = { osm, dist, nameSim };
      }
    }

    if (bestMatch) {
      matched++;
      const { osm, dist, nameSim } = bestMatch;

      // Build WKT polygon
      const points = osm.geometry.map(p => `${p.lon} ${p.lat}`);
      points.push(points[0]); // Close polygon
      const wkt = `POLYGON((${points.join(', ')}))`;

      try {
        await ds.query(`
          UPDATE apartment_complexes
          SET polygon = ST_GeomFromText($1, 4326),
              osm_id = $2,
              osm_type = $3,
              updated_at = NOW()
          WHERE id = $4
        `, [wkt, osm.id, osm.type, complex.id]);
        updated++;
        console.log(`✓ ${complex.name_ru} → ${osm.name} (sim=${nameSim.toFixed(2)}, dist=${Math.round(dist)}m)`);
      } catch (err) {
        console.log(`✗ ${complex.name_ru}: ${err.message}`);
      }
    }
  }

  console.log('\n=== Summary ===');
  console.log('Matched:', matched);
  console.log('Updated:', updated);

  const stats = await ds.query(`
    SELECT COUNT(*) as total, COUNT(polygon) as with_polygon FROM apartment_complexes
  `);
  console.log('Total with polygons:', stats[0].with_polygon, '/', stats[0].total);

  await ds.destroy();
}

main().catch(console.error);
