const { Client } = require('pg');
const https = require('https');
const http = require('http');

const DB_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  user: 'postgres',
  password: 'postgres'
};

const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://z.overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
];

let currentEndpointIndex = 0;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = deg => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Math.round(R * c);
}

function postOverpass(query, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const endpoint = new URL(OVERPASS_ENDPOINTS[currentEndpointIndex]);

    const options = {
      hostname: endpoint.hostname,
      port: endpoint.port || 443,
      path: endpoint.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(query),
      },
      timeout: timeoutMs,
    };

    const protocol = endpoint.protocol === 'https:' ? https : http;

    const req = protocol.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`Failed to parse response: ${data.substring(0, 200)}`));
        }
      });
    });

    req.on('error', err => {
      // Try next endpoint
      currentEndpointIndex = (currentEndpointIndex + 1) % OVERPASS_ENDPOINTS.length;
      reject(err);
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });

    req.write(query);
    req.end();
  });
}

async function getInfrastructure(lat, lng, radius = 1000) {
  const query = `
[out:json][timeout:25];
(
  nwr(around:${radius},${lat},${lng})["amenity"~"^(school|kindergarten)$"];
  nwr(around:${radius},${lat},${lng})["amenity"~"^(hospital|clinic)$"];
  nwr(around:${radius},${lat},${lng})["healthcare"~"^(hospital|clinic|doctor)$"];
  nwr(around:${radius},${lat},${lng})["shop"="supermarket"];
  nwr(around:${radius},${lat},${lng})["amenity"="parking"];
  nwr(around:${radius},${lat},${lng})["public_transport"~"^(stop_position|platform|station)$"];
  nwr(around:${radius},${lat},${lng})["highway"="bus_stop"];
  nwr(around:${radius},${lat},${lng})["railway"~"^(station|halt|tram_stop|subway_entrance)$"];
);
out center;`;

  try {
    const data = await postOverpass(query);
    const elements = data?.elements ?? [];

    const results = [];

    for (const el of elements) {
      const tags = el.tags ?? {};
      let type = null;

      if (tags.amenity === 'school') type = 'school';
      else if (tags.amenity === 'kindergarten') type = 'kindergarten';
      else if (tags.amenity === 'hospital' || tags.healthcare === 'hospital') type = 'hospital';
      else if (tags.amenity === 'clinic' || tags.healthcare === 'clinic' || tags.healthcare === 'doctor') type = 'hospital';
      else if (tags.shop === 'supermarket') type = 'supermarket';
      else if (tags.amenity === 'parking') type = 'parking';
      else if (tags.public_transport || tags.highway === 'bus_stop' || tags.railway) type = 'public_transport';

      if (!type) continue;

      const elLat = el.lat ?? el.center?.lat;
      const elLng = el.lon ?? el.center?.lon;
      if (!elLat || !elLng) continue;

      const distance = haversineDistance(lat, lng, elLat, elLng);
      const name = tags.name || tags['name:uk'] || tags['name:ru'];

      results.push({ lat: elLat, lng: elLng, type, distance, name });
    }

    return results.sort((a, b) => a.distance - b.distance);
  } catch (e) {
    console.error(`Error fetching infrastructure for ${lat},${lng}:`, e.message);
    return null;
  }
}

async function main() {
  const limit = parseInt(process.argv[2]) || 100;
  console.log(`Processing up to ${limit} listings...\n`);

  const client = new Client(DB_CONFIG);
  await client.connect();
  console.log('Connected to database\n');

  // Get listings without infrastructure
  const listings = await client.query(`
    SELECT id, lat, lng
    FROM unified_listings
    WHERE lat IS NOT NULL
      AND lng IS NOT NULL
      AND infrastructure IS NULL
    ORDER BY created_at DESC
    LIMIT $1
  `, [limit]);

  console.log(`Found ${listings.rows.length} listings to process\n`);

  let processed = 0;
  let updated = 0;
  let errors = 0;

  for (const row of listings.rows) {
    const lat = parseFloat(row.lat);
    const lng = parseFloat(row.lng);

    const infrastructure = await getInfrastructure(lat, lng);

    if (infrastructure === null) {
      errors++;
      await sleep(5000); // Wait longer on error
      continue;
    }

    if (infrastructure.length === 0) {
      processed++;
      continue;
    }

    // Find nearest of each type
    const nearestByType = {};
    for (const poi of infrastructure) {
      if (!nearestByType[poi.type]) {
        nearestByType[poi.type] = poi.distance;
      }
    }

    await client.query(`
      UPDATE unified_listings SET
        nearest_school = $2,
        nearest_hospital = $3,
        nearest_supermarket = $4,
        nearest_parking = $5,
        nearest_public_transport = $6,
        infrastructure = $7
      WHERE id = $1
    `, [
      row.id,
      nearestByType.school ?? nearestByType.kindergarten ?? null,
      nearestByType.hospital ?? null,
      nearestByType.supermarket ?? null,
      nearestByType.parking ?? null,
      nearestByType.public_transport ?? null,
      JSON.stringify(infrastructure.slice(0, 20)),
    ]);

    updated++;
    processed++;

    if (processed % 10 === 0) {
      console.log(`Processed: ${processed}/${listings.rows.length}, Updated: ${updated}, Errors: ${errors}`);
    }

    // Rate limiting: 1 request per second
    await sleep(1000);
  }

  console.log(`\n=== DONE ===`);
  console.log(`Processed: ${processed}`);
  console.log(`Updated: ${updated}`);
  console.log(`Errors: ${errors}`);

  // Show sample results
  const sample = await client.query(`
    SELECT nearest_school, nearest_hospital, nearest_supermarket, nearest_public_transport
    FROM unified_listings
    WHERE infrastructure IS NOT NULL
    LIMIT 5
  `);

  console.log('\nSample results:');
  sample.rows.forEach((r, i) => {
    console.log(`${i + 1}. School: ${r.nearest_school}m, Hospital: ${r.nearest_hospital}m, Supermarket: ${r.nearest_supermarket}m, Transport: ${r.nearest_public_transport}m`);
  });

  await client.end();
}

main().catch(console.error);
