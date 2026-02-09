const { Client } = require('pg');
const iconv = require('iconv-lite');

// Fix double-encoded text using iconv
function fixEncoding(str) {
  if (!str) return null;
  try {
    const cp1251Buffer = iconv.encode(str, 'win1251');
    const fixed = iconv.decode(cp1251Buffer, 'utf8');
    return fixed;
  } catch (e) {
    return str; // Return original if can't fix
  }
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Fixing Street Encoding ===\n');

  // Count streets with OSM ID (those that need fixing)
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM streets WHERE osm_id IS NOT NULL
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OSM streets to fix: ${totalCount}`);

  let processed = 0;
  let updated = 0;
  let offset = 0;
  const batchSize = 1000;
  const startTime = Date.now();

  while (processed < totalCount) {
    // Fetch batch
    const streets = await client.query(`
      SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru
      FROM streets
      WHERE osm_id IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (streets.rows.length === 0) break;

    // Fix encoding and prepare updates
    const updates = [];
    for (const s of streets.rows) {
      const fixedUk = fixEncoding(s.name_uk);
      const fixedRu = fixEncoding(s.name_ru);

      // Only update if something changed
      if (fixedUk !== s.name_uk || fixedRu !== s.name_ru) {
        updates.push({
          id: s.id,
          nameUk: fixedUk,
          nameRu: fixedRu,
        });
      }
    }

    // Bulk update
    if (updates.length > 0) {
      for (const u of updates) {
        await client.query(`
          UPDATE streets
          SET name = jsonb_build_object('uk', $1::text, 'ru', $2::text)
          WHERE id = $3
        `, [u.nameUk, u.nameRu, u.id]);
      }
      updated += updates.length;
    }

    processed += streets.rows.length;
    offset += batchSize;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`Progress: ${processed}/${totalCount} | Updated: ${updated} | ${elapsed}s`);
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nCompleted in ${totalTime}s. Updated ${updated} streets.`);

  // Verify fix
  console.log('\nVerifying fix (sample):');
  const verify = await client.query(`
    SELECT name->>'uk' as name_uk FROM streets WHERE osm_id IS NOT NULL LIMIT 5
  `);
  for (const v of verify.rows) {
    console.log('  ', v.name_uk);
  }

  await client.end();
}

main().catch(console.error);
