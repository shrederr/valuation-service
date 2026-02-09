const { Client } = require('pg');

async function main() {
  const c = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres',
  });

  await c.connect();

  const cities = ['Одеса', 'Львів', 'Київ', 'Харків', 'Дніпро', 'Вінниця', 'Чернівці', 'Ужгород', 'Запоріжжя'];

  console.log('Улицы в крупных городах:');
  console.log('='.repeat(60));

  for (const cityName of cities) {
    const r = await c.query(`
      SELECT
        g.id,
        g.name->>'uk' as name,
        g.type,
        COUNT(s.id) as streets_count,
        COUNT(s.id) FILTER (WHERE s.line IS NOT NULL) as with_geometry
      FROM geo g
      LEFT JOIN streets s ON s.geo_id = g.id
      WHERE g.name->>'uk' ILIKE $1
        AND g.type IN ('city', 'village')
      GROUP BY g.id
      ORDER BY COUNT(s.id) DESC
      LIMIT 1
    `, [`%${cityName}%`]);

    if (r.rows.length > 0) {
      const row = r.rows[0];
      const status = parseInt(row.streets_count) === 0 ? '❌ НЕТ УЛИЦ!' : '✅';
      console.log(`${row.name.padEnd(20)} ID:${row.id.toString().padEnd(6)} ${row.streets_count.toString().padStart(5)} улиц  ${status}`);
    } else {
      console.log(`${cityName.padEnd(20)} не найден в базе`);
    }
  }

  // Also check if Kyiv exists at all
  console.log('\nПоиск Киева в базе:');
  const kyiv = await c.query(`
    SELECT id, name->>'uk' as name, type, alias
    FROM geo
    WHERE name->>'uk' ILIKE '%київ%' OR name->>'uk' ILIKE '%киев%'
    LIMIT 10
  `);
  kyiv.rows.forEach(r => {
    console.log(`  ${r.id}: ${r.name} (${r.type}) alias=${r.alias}`);
  });

  await c.end();
}

main().catch(console.error);
