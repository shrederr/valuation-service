const { Client } = require('pg');

async function run() {
  const client = new Client({
    host: 'localhost',
    port: 5433,
    database: 'valuation',
    user: 'postgres',
    password: 'postgres'
  });

  await client.connect();

  const result = await client.query(`
    SELECT
      realty_platform as platform,
      COUNT(*) as total,
      COUNT(geo_id) as with_geo,
      COUNT(street_id) as with_street,
      COUNT(complex_id) as with_complex,
      COUNT(topzone_id) as with_topzone,
      COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as with_coords,
      COUNT(condition) as with_condition,
      COUNT(house_type) as with_house_type,
      COUNT(floor) as with_floor,
      COUNT(rooms) as with_rooms,
      COUNT(total_area) as with_area,
      ROUND(AVG(price)::numeric, 0) as avg_price,
      ROUND(AVG(total_area)::numeric, 1) as avg_area
    FROM unified_listings
    GROUP BY realty_platform
    ORDER BY COUNT(*) DESC
  `);

  const total = result.rows.reduce((s, r) => s + parseInt(r.total), 0);

  console.log('=== Полная статистика по платформам ===');
  console.log('Всего объектов:', total);
  console.log('');

  result.rows.forEach(r => {
    const cnt = parseInt(r.total);
    const pct = ((cnt / total) * 100).toFixed(1);
    const p = (v) => ((parseInt(v) / cnt) * 100).toFixed(0) + '%';

    console.log(`\n=== ${r.platform} ===`);
    console.log(`Объектов: ${cnt.toLocaleString()} (${pct}%)`);
    console.log(`Ср. цена: $${parseInt(r.avg_price).toLocaleString()} | Ср. площадь: ${r.avg_area} м²`);
    console.log('');
    console.log('ГЕО:');
    console.log(`  geo_id: ${p(r.with_geo)} | street_id: ${p(r.with_street)} | complex_id: ${p(r.with_complex)}`);
    console.log(`  topzone_id: ${p(r.with_topzone)} | координаты: ${p(r.with_coords)}`);
    console.log('');
    console.log('ХАРАКТЕРИСТИКИ:');
    console.log(`  condition: ${p(r.with_condition)} | house_type: ${p(r.with_house_type)}`);
    console.log(`  floor: ${p(r.with_floor)} | rooms: ${p(r.with_rooms)} | area: ${p(r.with_area)}`);
  });

  await client.end();
}

run().catch(console.error);
