const { Client } = require('pg');
const iconv = require('iconv-lite');

function fixEncoding(str) {
  if (!str) return str;
  try {
    const buf = iconv.encode(str, 'win1251');
    return iconv.decode(buf, 'utf8');
  } catch (e) {
    return str;
  }
}

function fixNamesObject(names) {
  if (!names) return names;

  const fixed = {};
  for (const [lang, arr] of Object.entries(names)) {
    if (Array.isArray(arr)) {
      fixed[lang] = arr.map(s => fixEncoding(s));
    } else {
      fixed[lang] = arr;
    }
  }
  return fixed;
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Исправление кодировки в поле names ===\n');

  // Получим все улицы с names
  const countResult = await client.query(`SELECT COUNT(*) as cnt FROM streets WHERE names IS NOT NULL`);
  const total = parseInt(countResult.rows[0].cnt);
  console.log(`Всего улиц с names: ${total}`);

  const batchSize = 500;
  let updated = 0;
  let offset = 0;

  while (offset < total) {
    const batch = await client.query(`
      SELECT id, names
      FROM streets
      WHERE names IS NOT NULL
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    for (const row of batch.rows) {
      const fixedNames = fixNamesObject(row.names);

      await client.query(`
        UPDATE streets SET names = $1 WHERE id = $2
      `, [JSON.stringify(fixedNames), row.id]);

      updated++;
    }

    console.log(`Обновлено: ${updated}/${total}`);
    offset += batchSize;
  }

  console.log('\n--- Проверка результата ---');
  const check = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets
    WHERE jsonb_array_length(names->'uk') > 1
    LIMIT 5
  `);

  check.rows.forEach(r => {
    console.log(`[${r.id}] ${r.name_uk}`);
    console.log(`  names.uk: ${JSON.stringify(r.uk_names)}`);
  });

  await client.end();
  console.log('\nГотово!');
}

main().catch(console.error);
