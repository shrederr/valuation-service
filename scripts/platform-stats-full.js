const { Client } = require('pg');

async function analyze() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'aggregator_dump',
    user: 'postgres',
    password: 'postgres'
  });

  await client.connect();

  const result = await client.query(`
    SELECT
      realty_platform as platform,
      COUNT(*) as total,
      COUNT(street_id) as with_street,
      COUNT(complex_id) as with_complex,
      COUNT(condition) as with_condition,
      COUNT(CASE WHEN attributes::text LIKE '%project%' THEN 1 END) as with_house_type,
      COUNT(floor) as with_floor,
      COUNT(rooms) as with_rooms,
      ROUND(AVG(price::numeric), 0) as avg_price
    FROM ready_for_export_all
    GROUP BY realty_platform
    ORDER BY COUNT(*) DESC
  `);

  const total = result.rows.reduce((sum, r) => sum + parseInt(r.total), 0);

  console.log('=== Статистика по платформам (aggregator_dump) ===');
  console.log('Всего записей:', total);
  console.log('');

  result.rows.forEach(r => {
    const cnt = parseInt(r.total);
    const pct = ((cnt / total) * 100).toFixed(1);
    const streetPct = ((parseInt(r.with_street) / cnt) * 100).toFixed(0);
    const complexPct = ((parseInt(r.with_complex) / cnt) * 100).toFixed(0);
    const condPct = ((parseInt(r.with_condition) / cnt) * 100).toFixed(0);
    const housePct = ((parseInt(r.with_house_type) / cnt) * 100).toFixed(0);
    const floorPct = ((parseInt(r.with_floor) / cnt) * 100).toFixed(0);
    const roomsPct = ((parseInt(r.with_rooms) / cnt) * 100).toFixed(0);

    console.log(`--- ${r.platform} ---`);
    console.log(`  Записей: ${r.total} (${pct}%)`);
    console.log(`  street: ${streetPct}% | complex: ${complexPct}%`);
    console.log(`  condition: ${condPct}% | house_type: ${housePct}%`);
    console.log(`  floor: ${floorPct}% | rooms: ${roomsPct}%`);
    console.log(`  Ср. цена: $${r.avg_price || 'N/A'}`);
    console.log('');
  });

  await client.end();
}

analyze().catch(e => console.error(e.message));
