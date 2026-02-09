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
    return str;
  }
}

// Escape single quotes for SQL
function escapeSQL(str) {
  if (!str) return 'NULL';
  return "'" + str.replace(/'/g, "''") + "'";
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Fixing Street Encoding (Fast) ===\n');

  // Count streets with OSM ID
  const countResult = await client.query(`
    SELECT COUNT(*) as cnt FROM streets WHERE osm_id IS NOT NULL
  `);
  const totalCount = parseInt(countResult.rows[0].cnt, 10);
  console.log(`Total OSM streets to fix: ${totalCount}`);

  let processed = 0;
  let offset = 0;
  const batchSize = 500;
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

    // Build bulk update using VALUES
    const values = streets.rows.map(s => {
      const fixedUk = fixEncoding(s.name_uk);
      const fixedRu = fixEncoding(s.name_ru);
      return `(${s.id}, ${escapeSQL(fixedUk)}, ${escapeSQL(fixedRu)})`;
    }).join(',\n');

    // Bulk update
    await client.query(`
      UPDATE streets s
      SET name = jsonb_build_object('uk', v.name_uk, 'ru', v.name_ru)
      FROM (VALUES ${values}) AS v(id, name_uk, name_ru)
      WHERE s.id = v.id
    `);

    processed += streets.rows.length;
    offset += batchSize;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const rate = Math.round(processed / (parseFloat(elapsed) || 1));
    console.log(`Progress: ${processed}/${totalCount} (${((processed/totalCount)*100).toFixed(1)}%) | ${elapsed}s (${rate}/s)`);
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nCompleted in ${totalTime}s`);

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
