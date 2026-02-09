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

  // Count OLX with street patterns
  const r = await c.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE street_id IS NULL) as no_street,
      COUNT(*) FILTER (WHERE street_id IS NULL AND (
        description->>'uk' ~* 'вул\\.|вулиця|ул\\.|улица|пр-т|проспект|бульвар|б-р|провулок|пров\\.'
      )) as with_pattern
    FROM unified_listings
    WHERE realty_platform = 'olx'
  `);

  console.log('OLX статистика:');
  console.log('Всего:', r.rows[0].total);
  console.log('Без street_id:', r.rows[0].no_street);
  console.log('Без street_id + есть паттерн улицы:', r.rows[0].with_pattern);

  await c.end();
}

main().catch(console.error);
