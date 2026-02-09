const { Client } = require('pg');

// Исправление двойной UTF-8 кодировки
function fixDoubleUtf8(str) {
  if (!str) return str;
  try {
    return Buffer.from(str, 'latin1').toString('utf8');
  } catch (e) {
    return str;
  }
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Проверка исправления кодировки ===\n');

  // Тестовая строка
  const broken = "РІСѓР»РёС†СЏ РџРµС‚СЂР° Р›РµС‰РµРЅРєР°";
  const fixed = fixDoubleUtf8(broken);
  console.log('Broken:', broken);
  console.log('Fixed:', fixed);

  // Проверим на реальных данных
  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets
    WHERE jsonb_array_length(names->'uk') > 1
    LIMIT 5
  `);

  console.log('\n--- Тест на реальных данных ---');
  for (const row of result.rows) {
    console.log(`\n[${row.id}] ${row.name_uk}`);
    const ukNames = row.uk_names || [];
    for (const name of ukNames) {
      const fixedName = fixDoubleUtf8(name);
      console.log(`  Broken: ${name}`);
      console.log(`  Fixed:  ${fixedName}`);
    }
  }

  await client.end();
}

main().catch(console.error);
