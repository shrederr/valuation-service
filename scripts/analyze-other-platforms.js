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

  const platforms = ['olx', 'domRia'];

  for (const platform of platforms) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`ПЛАТФОРМА: ${platform}`);
    console.log('='.repeat(60));

    // Пример primary_data
    console.log('\n--- ПРИКЛАД primary_data ---');
    const example = await client.query(`
      SELECT id, primary_data
      FROM aggregator_import
      WHERE realty_platform = $1
        AND primary_data IS NOT NULL
        AND primary_data != ''
      LIMIT 1
    `, [platform]);

    if (example.rows.length > 0) {
      const data = typeof example.rows[0].primary_data === 'string'
        ? JSON.parse(example.rows[0].primary_data)
        : example.rows[0].primary_data;
      console.log('ID:', example.rows[0].id);
      console.log('Ключі primary_data:', Object.keys(data).join(', '));
      console.log('\nПовні дані:');
      console.log(JSON.stringify(data, null, 2).substring(0, 2000));
    } else {
      console.log('Немає записів з primary_data');
    }

    // Кількість записів
    const count = await client.query(`
      SELECT COUNT(*) as cnt FROM aggregator_import WHERE realty_platform = $1
    `, [platform]);
    console.log(`\nВсього записів: ${count.rows[0].cnt}`);

    // Записи з primary_data
    const withPrimary = await client.query(`
      SELECT COUNT(*) as cnt FROM aggregator_import
      WHERE realty_platform = $1 AND primary_data IS NOT NULL AND primary_data != ''
    `, [platform]);
    console.log(`З primary_data: ${withPrimary.rows[0].cnt}`);
  }

  await client.end();
}
main().catch(console.error);
