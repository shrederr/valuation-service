const { Client } = require('pg');
const fs = require('fs');

function approxDistanceKm(lat1, lng1, lat2, lng2) {
  const latDiff = (lat2 - lat1) * 111.32;
  const lngDiff = (lng2 - lng1) * 111.32 * Math.cos(lat1 * Math.PI / 180);
  return Math.sqrt(latDiff * latDiff + lngDiff * lngDiff);
}

function normalizeForComparison(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/^(жк|кг|км|житловий комплекс|жилой комплекс|котеджне містечко|коттеджный городок|таунхаус[иі]?|дуплекс[иі]?|клубний будинок|клубный дом|апарт.комплекс|апарт.готель)\s*/gi, '')
    .replace(/[«»""''`'()]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Import GeoVector Complexes ===\n');

  // Read CSV
  const csvPath = 'D:\\analogis\\GeoVector.csv';
  const csvData = fs.readFileSync(csvPath, 'utf8').split('\n').slice(1).filter(Boolean);
  console.log(`CSV records: ${csvData.length}`);

  // Parse CSV
  const geoVectorComplexes = [];
  for (const line of csvData) {
    // Handle quoted fields with commas
    const parts = line.match(/(".*?"|[^,]+)(?=\s*,|\s*$)/g) || [];
    if (parts.length < 5) continue;

    const ru = (parts[0] || '').replace(/^"|"$/g, '').trim();
    const uk = (parts[1] || '').replace(/^"|"$/g, '').trim();
    const en = (parts[2] || '').replace(/^"|"$/g, '').trim();
    const lat = parseFloat(parts[3]);
    const lng = parseFloat(parts[4]);

    if (!ru && !uk) continue;
    if (isNaN(lat) || isNaN(lng)) continue;

    geoVectorComplexes.push({ ru, uk, en, lat, lng });
  }
  console.log(`Parsed complexes: ${geoVectorComplexes.length}`);

  // Load existing complexes from DB
  console.log('\nLoading existing complexes from DB...');
  const existing = await client.query(`
    SELECT id, name_ru, name_uk, lat, lng FROM apartment_complexes
  `);
  console.log(`Existing in DB: ${existing.rows.length}`);

  // Build lookup for existing
  const existingByName = new Map();
  const existingCoords = [];

  for (const e of existing.rows) {
    const normRu = normalizeForComparison(e.name_ru);
    const normUk = normalizeForComparison(e.name_uk);
    if (normRu) existingByName.set(normRu, e.id);
    if (normUk && normUk !== normRu) existingByName.set(normUk, e.id);
    existingCoords.push({
      id: e.id,
      lat: parseFloat(e.lat),
      lng: parseFloat(e.lng)
    });
  }

  // Find missing complexes
  const missing = [];
  let foundByName = 0;
  let foundByCoords = 0;

  for (const gv of geoVectorComplexes) {
    const normRu = normalizeForComparison(gv.ru);
    const normUk = normalizeForComparison(gv.uk);

    // Check by name
    if (existingByName.has(normRu) || existingByName.has(normUk)) {
      foundByName++;
      continue;
    }

    // Check by coordinates (within 100m)
    let foundNearby = false;
    for (const ec of existingCoords) {
      if (approxDistanceKm(gv.lat, gv.lng, ec.lat, ec.lng) < 0.1) {
        foundByCoords++;
        foundNearby = true;
        break;
      }
    }

    if (!foundNearby) {
      missing.push(gv);
    }
  }

  console.log(`\nFound by name: ${foundByName}`);
  console.log(`Found by coords: ${foundByCoords}`);
  console.log(`Missing (to import): ${missing.length}`);

  if (missing.length === 0) {
    console.log('\nAll complexes already exist!');
    await client.end();
    return;
  }

  // Show sample of missing
  console.log('\n=== Sample missing complexes ===');
  for (const m of missing.slice(0, 20)) {
    console.log(`  ${m.ru} (${m.lat}, ${m.lng})`);
  }

  // Load streets for enrichment
  console.log('\nLoading streets for enrichment...');
  const streetsResult = await client.query(`
    SELECT id, geo_id, ST_Y(ST_Centroid(line)) as lat, ST_X(ST_Centroid(line)) as lng
    FROM streets WHERE line IS NOT NULL AND geo_id IS NOT NULL
  `);
  const streets = streetsResult.rows.map(s => ({
    id: s.id,
    geo_id: s.geo_id,
    lat: parseFloat(s.lat),
    lng: parseFloat(s.lng)
  }));
  console.log(`Loaded ${streets.length} streets`);

  // Insert missing complexes
  console.log('\n=== Inserting missing complexes ===');
  let inserted = 0;
  let enriched = 0;

  for (const m of missing) {
    // Find nearest street
    let nearestStreet = null;
    let minDist = Infinity;

    for (const s of streets) {
      const dist = approxDistanceKm(m.lat, m.lng, s.lat, s.lng);
      if (dist < minDist && dist <= 2) {
        minDist = dist;
        nearestStreet = s;
      }
    }

    // Create normalized name
    const normalized = normalizeForComparison(m.ru) || normalizeForComparison(m.uk);

    try {
      await client.query(`
        INSERT INTO apartment_complexes (name_ru, name_uk, name_en, name_normalized, lat, lng, street_id, geo_id, source, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'geovector_import', NOW(), NOW())
      `, [
        m.ru || null,
        m.uk || null,
        m.en || null,
        normalized,
        m.lat,
        m.lng,
        nearestStreet ? nearestStreet.id : null,
        nearestStreet ? nearestStreet.geo_id : null
      ]);

      inserted++;
      if (nearestStreet) enriched++;

      if (inserted % 100 === 0) {
        console.log(`Inserted: ${inserted}/${missing.length}, enriched: ${enriched}`);
      }
    } catch (err) {
      console.error(`Error inserting ${m.ru}: ${err.message}`);
    }
  }

  console.log(`\n=== Done ===`);
  console.log(`Inserted: ${inserted}`);
  console.log(`With street/geo: ${enriched}`);

  // Final stats
  const stats = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(CASE WHEN source = 'geovector_import' THEN 1 END) as new_import
    FROM apartment_complexes
  `);
  console.log(`\nFinal DB stats:`);
  console.log(`Total complexes: ${stats.rows[0].total}`);
  console.log(`With street_id: ${stats.rows[0].with_street}`);
  console.log(`New from this import: ${stats.rows[0].new_import}`);

  await client.end();
}

main().catch(console.error);
