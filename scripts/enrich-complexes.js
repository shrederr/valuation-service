const { Client } = require('pg');

// Blacklist - not residential complexes
const BLACKLIST_PATTERNS = [
  // Shops
  /сільпо|novus|varus|атб|фора|ашан|магазин|супермаркет|гіпермаркет|продукти|metro|lidl|wog|okko|укрнафта/i,
  // Banks
  /банк|ощад|приват|укрексім|промінвест|sense bank|креді агріколь|альфа.банк|monobank/i,
  // Religious
  /церква|храм|собор|мечеть|синагога|монастир|каплиця|костел/i,
  // Education
  /школа|садок|ліцей|гімназія|університет|коледж|технікум|інститут|академія|дюсш/i,
  // Medical
  /лікарня|поліклініка|медичн|стоматолог|аптека|клініка|діагностич/i,
  // Food/Entertainment
  /ресторан|кафе|бар|паб|піцерія|mcdonalds|kfc|пузата хата/i,
  // Infrastructure
  /котельня|теплопункт|підстанція|трансформатор|насосна|очисн|водоканал/i,
  // Parking/Storage
  /паркінг|стоянка|гараж|склад|ангар/i,
  // Offices
  /бізнес.центр|офіс|торгов.*центр|торгов.*комплекс|молл|plaza/i,
  // Government
  /поліція|військомат|комісаріат|адміністрац|рада|суд|прокуратур|мвс|сбу|цнап/i,
  // Памятники архитектуры (OSM)
  /пам'ятка архітектури|прибутковий будинок/i,
  // Gas stations
  /азс|запрвка|бензин/i,
  // Generic short
  /^(кпп|сто|фап|упг)$/i,
];

// Valid prefixes for residential complexes
const VALID_PREFIXES = [
  /^жк\s/i,
  /^кг\s/i,
  /^км\s/i,
  /^житловий комплекс/i,
  /^жилой комплекс/i,
  /^жилий комплекс/i,
  /^котеджн/i,
  /^таунхаус/i,
  /^апарт.комплекс/i,
  /^клубн.*будинок/i,
  /^клубн.*дом/i,
  /^жилой.*парк/i,
  /^житловий.*парк/i,
  /^жилой.*городок/i,
  /^житлове.*містечко/i,
  /^мікрорайон/i,
  /^микрорайон/i,
];

function isValidComplex(nameRu, nameUk, source) {
  const name = (nameRu || nameUk || '').toLowerCase();

  // Check blacklist
  for (const pattern of BLACKLIST_PATTERNS) {
    if (pattern.test(name)) return false;
  }

  // Too short name (after removing prefix)
  const cleanedName = name.replace(/^(жк|кг|км)\s+/i, '');
  if (cleanedName.length < 3) return false;

  // Single number or very short
  if (/^\d+$/.test(cleanedName)) return false;

  // geovector source is trusted
  if (source === 'geovector' || source === 'merged') return true;

  // For OSM, require valid prefix
  const fullName = nameRu + ' ' + (nameUk || '');
  for (const prefix of VALID_PREFIXES) {
    if (prefix.test(fullName)) return true;
  }

  return false;
}

function approxDistanceKm(lat1, lng1, lat2, lng2) {
  const latDiff = (lat2 - lat1) * 111.32;
  const lngDiff = (lng2 - lng1) * 111.32 * Math.cos(lat1 * Math.PI / 180);
  return Math.sqrt(latDiff * latDiff + lngDiff * lngDiff);
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Enriching Apartment Complexes ===\n');

  // Load all streets
  console.log('Loading streets...');
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
  console.log(`Loaded ${streets.length} streets with geo_id\n`);

  // Load all complexes
  console.log('Loading complexes...');
  const complexesResult = await client.query(`
    SELECT id, name_ru, name_uk, lat, lng, source
    FROM apartment_complexes
    WHERE lat IS NOT NULL AND lng IS NOT NULL
  `);
  console.log(`Total complexes: ${complexesResult.rows.length}\n`);

  // Filter valid complexes
  let validCount = 0;
  let invalidCount = 0;
  const invalidExamples = [];
  const updates = [];

  for (const c of complexesResult.rows) {
    const isValid = isValidComplex(c.name_ru, c.name_uk, c.source);

    if (!isValid) {
      invalidCount++;
      if (invalidExamples.length < 30) {
        invalidExamples.push(c.name_ru || c.name_uk);
      }
      continue;
    }

    validCount++;

    // Find nearest street
    const cLat = parseFloat(c.lat);
    const cLng = parseFloat(c.lng);

    let nearestStreet = null;
    let minDist = Infinity;

    for (const s of streets) {
      const dist = approxDistanceKm(cLat, cLng, s.lat, s.lng);
      if (dist < minDist && dist <= 2) { // max 2km
        minDist = dist;
        nearestStreet = s;
      }
    }

    if (nearestStreet) {
      updates.push({
        id: c.id,
        street_id: nearestStreet.id,
        geo_id: nearestStreet.geo_id
      });
    }
  }

  console.log('=== Filtering Results ===');
  console.log(`Valid complexes: ${validCount}`);
  console.log(`Invalid (filtered out): ${invalidCount}`);
  console.log(`With nearest street found: ${updates.length}`);

  console.log('\n=== Sample filtered out ===');
  for (const e of invalidExamples.slice(0, 20)) {
    console.log(`  ${e}`);
  }

  // Update complexes with street_id and geo_id
  console.log('\n=== Updating complexes ===');
  const batchSize = 500;
  let updated = 0;

  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    const values = batch.map(u => `(${u.id}, ${u.street_id}, ${u.geo_id})`).join(',');

    await client.query(`
      UPDATE apartment_complexes ac
      SET street_id = v.street_id, geo_id = v.geo_id
      FROM (VALUES ${values}) AS v(id, street_id, geo_id)
      WHERE ac.id = v.id
    `);

    updated += batch.length;
    console.log(`Updated: ${updated}/${updates.length}`);
  }

  // Final stats
  console.log('\n=== Final Stats ===');
  const stats = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(geo_id) as with_geo
    FROM apartment_complexes
  `);

  const r = stats.rows[0];
  console.log(`Total: ${r.total}`);
  console.log(`With street_id: ${r.with_street} (${((r.with_street/r.total)*100).toFixed(1)}%)`);
  console.log(`With geo_id: ${r.with_geo} (${((r.with_geo/r.total)*100).toFixed(1)}%)`);

  await client.end();
}

main().catch(console.error);
