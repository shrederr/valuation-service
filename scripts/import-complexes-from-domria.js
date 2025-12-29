const { DataSource } = require('typeorm');
require('dotenv').config();

const ds = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
});

// Normalize name for comparison
function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/жк|ж\.к\.|житловий комплекс|жилой комплекс/gi, '')
    .replace(/["«»''№#]/g, '')
    .replace(/[^\wа-яіїєґ0-9\s]/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Check if names are similar (handle variations like "Акварель 8" vs "Акварель-8")
function isSimilar(name1, name2) {
  const n1 = normalizeName(name1);
  const n2 = normalizeName(name2);

  if (n1 === n2) return true;
  if (n1.includes(n2) || n2.includes(n1)) return true;

  // Remove numbers and compare base names
  const base1 = n1.replace(/\d+/g, '').trim();
  const base2 = n2.replace(/\d+/g, '').trim();
  if (base1.length > 3 && base2.length > 3 && (base1.includes(base2) || base2.includes(base1))) {
    // Check if numbers match too
    const nums1 = n1.match(/\d+/g) || [];
    const nums2 = n2.match(/\d+/g) || [];
    if (nums1.join('') === nums2.join('')) return true;
  }

  return false;
}

// Transliterate common variations
function getVariations(name) {
  const n = normalizeName(name);
  const variations = [n];

  // Common replacements
  const replacements = [
    [/ы/g, 'и'], [/и/g, 'і'], [/і/g, 'и'],
    [/е/g, 'є'], [/є/g, 'е'],
    [/ё/g, 'е'],
    [/ь/g, ''],
    [/\s+/g, ''], // No spaces version
    [/(\d+)/g, ' $1 '], // Spaces around numbers
  ];

  for (const [from, to] of replacements) {
    const v = n.replace(from, to).trim();
    if (v !== n && v.length > 2) variations.push(v);
  }

  return variations;
}

async function main() {
  await ds.initialize();
  console.log('=== Import ЖК from dom.ria ===\n');

  // Get existing complexes
  const existing = await ds.query(`SELECT id, name_ru, name_uk, name_normalized, lat, lng FROM apartment_complexes`);
  const existingNames = [];
  existing.forEach(e => {
    existingNames.push({
      id: e.id,
      names: [e.name_ru, e.name_uk, e.name_normalized].filter(Boolean),
      lat: parseFloat(e.lat),
      lng: parseFloat(e.lng),
    });
  });
  console.log('Existing complexes:', existing.length);

  // Check if name matches any existing complex
  function findExisting(name, lat, lng) {
    for (const e of existingNames) {
      for (const eName of e.names) {
        if (isSimilar(name, eName)) {
          return e.id;
        }
      }
    }
    // Also check by proximity (within 100m)
    if (lat && lng) {
      for (const e of existingNames) {
        if (e.lat && e.lng) {
          const dist = Math.sqrt(Math.pow((lat - e.lat) * 111000, 2) + Math.pow((lng - e.lng) * 111000 * 0.7, 2));
          if (dist < 100) return e.id; // Within 100m
        }
      }
    }
    return null;
  }

  // Get dom.ria listings with ЖК and coordinates
  const listings = await ds.query(`
    SELECT id, description::text as description, lat, lng, geo_id
    FROM unified_listings
    WHERE (external_url LIKE '%dom.ria%' OR external_url LIKE '%domria%')
      AND lat IS NOT NULL AND lng IS NOT NULL
      AND (
        description::text ILIKE '%жк %' OR
        description::text ILIKE '%жк"%' OR
        description::text ILIKE '%жк«%' OR
        description::text ILIKE '% жк%'
      )
  `);
  console.log('Dom.ria listings with ЖК and coords:', listings.length);

  // Extract ЖК names with coordinates
  const complexCandidates = new Map();

  // Pattern to extract ЖК name - more precise
  const patterns = [
    /жк\s*[«"']([^«"']{3,40})[»"']/gi,  // ЖК "Name" or ЖК «Name»
    /жк\s+([а-яіїєґa-z0-9][а-яіїєґa-z0-9\s\-]{2,25}?)(?:\s*[,.\n\r]|$|\s+на\s|\s+от\s|\s+в\s|\s+по\s)/gi,  // ЖК Name,
    /(?:в|у)\s+жк\s+([а-яіїєґa-z0-9][а-яіїєґa-z0-9\s\-]{2,25}?)(?:\s*[,.\n\r]|$)/gi,  // в ЖК Name
  ];

  for (const listing of listings) {
    const text = String(listing.description || '');

    for (const pattern of patterns) {
      pattern.lastIndex = 0;
      let match;
      while ((match = pattern.exec(text)) !== null) {
        let name = match[1].trim();

        // Clean up
        name = name.replace(/^[\s,.\-:]+|[\s,.\-:]+$/g, '');
        name = name.replace(/\s+/g, ' ');

        // Skip garbage
        if (name.length < 3 || name.length > 35) continue;
        if (/^(на|в|с|от|по|для|это|эт|и|или|с|для|закрытой|закритої|с\s|із\s)$/i.test(name)) continue;
        if (/^(а пола|а підлоги|закрытой|закритою)/i.test(name)) continue;

        const normalized = normalizeName(name);
        if (normalized.length < 3) continue;

        // Check if already exists in DB
        const avgLat = listing.lat ? parseFloat(listing.lat) : null;
        const avgLng = listing.lng ? parseFloat(listing.lng) : null;
        if (findExisting(name, avgLat, avgLng)) continue;

        // Add to candidates
        if (!complexCandidates.has(normalized)) {
          complexCandidates.set(normalized, {
            name: name,
            normalized: normalized,
            coords: [],
            geoIds: new Set(),
            count: 0,
          });
        }
        const candidate = complexCandidates.get(normalized);
        candidate.count++;
        if (listing.lat && listing.lng) {
          candidate.coords.push({ lat: parseFloat(listing.lat), lng: parseFloat(listing.lng) });
        }
        if (listing.geo_id) {
          candidate.geoIds.add(listing.geo_id);
        }
      }
    }
  }

  // Filter candidates - need at least 3 mentions and coords, and double-check for duplicates
  const validCandidates = [...complexCandidates.values()]
    .filter(c => c.count >= 3 && c.coords.length >= 2)
    .filter(c => {
      // Double check not similar to existing
      const avgLat = c.coords.reduce((s, p) => s + p.lat, 0) / c.coords.length;
      const avgLng = c.coords.reduce((s, p) => s + p.lng, 0) / c.coords.length;
      return !findExisting(c.name, avgLat, avgLng);
    })
    .sort((a, b) => b.count - a.count);

  console.log('\nValid new ЖК candidates (2+ mentions with coords):', validCandidates.length);

  // Show top candidates
  console.log('\nTop 30 new ЖК to add:');
  validCandidates.slice(0, 30).forEach((c, i) => {
    // Calculate centroid
    const avgLat = c.coords.reduce((s, p) => s + p.lat, 0) / c.coords.length;
    const avgLng = c.coords.reduce((s, p) => s + p.lng, 0) / c.coords.length;
    console.log(`  ${i+1}. "${c.name}" (${c.count} mentions, ${c.coords.length} coords) -> [${avgLat.toFixed(5)}, ${avgLng.toFixed(5)}]`);
  });

  // Insert new complexes
  console.log('\n\n=== Inserting new complexes ===\n');
  let inserted = 0;

  for (const c of validCandidates) {
    // Calculate centroid
    const avgLat = c.coords.reduce((s, p) => s + p.lat, 0) / c.coords.length;
    const avgLng = c.coords.reduce((s, p) => s + p.lng, 0) / c.coords.length;

    // Get most common geo_id
    const geoId = c.geoIds.size > 0 ? [...c.geoIds][0] : null;

    try {
      await ds.query(`
        INSERT INTO apartment_complexes (name_ru, name_uk, name_normalized, lat, lng, geo_id, source, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'domria', NOW(), NOW())
        ON CONFLICT DO NOTHING
      `, [c.name, c.name, c.normalized, avgLat, avgLng, geoId]);
      inserted++;
    } catch (err) {
      console.log(`  Error inserting "${c.name}":`, err.message);
    }
  }

  console.log('Inserted new complexes:', inserted);

  // Final count
  const finalCount = await ds.query(`SELECT COUNT(*) as cnt FROM apartment_complexes`);
  console.log('Total complexes now:', finalCount[0].cnt);

  // Show by source
  const bySource = await ds.query(`
    SELECT source, COUNT(*) as cnt FROM apartment_complexes GROUP BY source ORDER BY cnt DESC
  `);
  console.log('\nBy source:', bySource);

  await ds.destroy();
}

main().catch(console.error);
