const { Client } = require('pg');

async function main() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });
  await client.connect();

  // ================= OLX =================
  console.log('\n' + '='.repeat(60));
  console.log('OLX: АНАЛІЗ params');
  console.log('='.repeat(60));

  // Знаходимо всі унікальні ключі params
  const olxParams = await client.query(`
    SELECT DISTINCT jsonb_array_elements(primary_data::jsonb->'params')->>'key' as param_key
    FROM aggregator_import
    WHERE realty_platform = 'olx'
      AND primary_data IS NOT NULL
      AND primary_data != ''
    LIMIT 1000
  `);

  console.log('\nУнікальні ключі params в OLX:');
  const olxKeys = olxParams.rows.map(r => r.param_key).filter(Boolean);
  console.log(olxKeys.join('\n'));

  // Шукаємо значення для типу будинку
  console.log('\n--- property_type (тип будинку) ---');
  const olxHouseTypes = await client.query(`
    SELECT
      elem->>'value' as value,
      elem->>'normalizedValue' as normalized,
      COUNT(*) as cnt
    FROM aggregator_import,
         jsonb_array_elements(primary_data::jsonb->'params') as elem
    WHERE realty_platform = 'olx'
      AND elem->>'key' = 'property_type_appartments_sale'
    GROUP BY elem->>'value', elem->>'normalizedValue'
    ORDER BY cnt DESC
    LIMIT 20
  `);
  for (const row of olxHouseTypes.rows) {
    console.log(`${row.value} (${row.normalized}): ${row.cnt}`);
  }

  // Шукаємо стан квартири
  console.log('\n--- state/condition (стан) ---');
  const olxConditions = await client.query(`
    SELECT
      elem->>'key' as key,
      elem->>'value' as value,
      COUNT(*) as cnt
    FROM aggregator_import,
         jsonb_array_elements(primary_data::jsonb->'params') as elem
    WHERE realty_platform = 'olx'
      AND (elem->>'key' LIKE '%state%' OR elem->>'key' LIKE '%condition%' OR elem->>'key' LIKE '%repair%' OR elem->>'key' LIKE '%remont%')
    GROUP BY elem->>'key', elem->>'value'
    ORDER BY cnt DESC
    LIMIT 30
  `);
  for (const row of olxConditions.rows) {
    console.log(`[${row.key}] ${row.value}: ${row.cnt}`);
  }

  // ================= domRia =================
  console.log('\n' + '='.repeat(60));
  console.log('domRia: АНАЛІЗ полів');
  console.log('='.repeat(60));

  // wall_type
  console.log('\n--- wall_type ---');
  const domRiaWallTypes = await client.query(`
    SELECT
      primary_data::jsonb->>'wall_type' as wall_type,
      COUNT(*) as cnt
    FROM aggregator_import
    WHERE realty_platform = 'domRia'
      AND primary_data IS NOT NULL
    GROUP BY primary_data::jsonb->>'wall_type'
    ORDER BY cnt DESC
    LIMIT 20
  `);
  for (const row of domRiaWallTypes.rows) {
    console.log(`${row.wall_type || 'null'}: ${row.cnt}`);
  }

  // characteristics_values - шукаємо стан
  console.log('\n--- characteristics_values (приклад) ---');
  const domRiaChars = await client.query(`
    SELECT id, primary_data::jsonb->'characteristics_values' as chars
    FROM aggregator_import
    WHERE realty_platform = 'domRia'
      AND primary_data::jsonb->'characteristics_values' IS NOT NULL
      AND primary_data::jsonb->'characteristics_values' != 'null'
    LIMIT 3
  `);
  for (const row of domRiaChars.rows) {
    console.log(`\nID ${row.id}:`, JSON.stringify(row.chars, null, 2).substring(0, 500));
  }

  await client.end();
}
main().catch(console.error);
