const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  console.log('=== Перепривязка улиц к самому нижнему geo (FIX) ===\n');

  // Проверим сколько улиц нужно обновить
  const stats = await client.query(`
    SELECT COUNT(*) as total,
           COUNT(*) FILTER (WHERE s.geo_id != best.best_geo_id) as need_update
    FROM streets s
    CROSS JOIN LATERAL (
      SELECT g.id as best_geo_id
      FROM geo g
      WHERE g.polygon IS NOT NULL
        AND s.line IS NOT NULL
        AND ST_Contains(g.polygon, ST_Centroid(s.line))
      ORDER BY g.lft ASC
      LIMIT 1
    ) best
  `);

  console.log(`Всего улиц с геометрией: ${stats.rows[0].total}`);
  console.log(`Нужно обновить: ${stats.rows[0].need_update}\n`);

  // Обновляем батчами
  const batchSize = 1000;
  let updated = 0;

  while (true) {
    const result = await client.query(`
      WITH streets_to_update AS (
        SELECT s.id, best.best_geo_id
        FROM streets s
        CROSS JOIN LATERAL (
          SELECT g.id as best_geo_id
          FROM geo g
          WHERE g.polygon IS NOT NULL
            AND s.line IS NOT NULL
            AND ST_Contains(g.polygon, ST_Centroid(s.line))
          ORDER BY g.lft ASC
          LIMIT 1
        ) best
        WHERE s.geo_id != best.best_geo_id
        LIMIT $1
      )
      UPDATE streets s
      SET geo_id = stu.best_geo_id
      FROM streets_to_update stu
      WHERE s.id = stu.id
      RETURNING s.id
    `, [batchSize]);

    if (result.rowCount === 0) break;

    updated += result.rowCount;
    console.log(`Обновлено: ${updated}`);
  }

  console.log(`\nГотово! Всего обновлено: ${updated}`);

  // Проверим результат
  console.log('\n=== Проверка ===');
  const check = await client.query(`
    SELECT s.id, s.name->>'uk' as street, s.geo_id,
           g.name->>'uk' as geo_name, g.type
    FROM streets s
    LEFT JOIN geo g ON s.geo_id = g.id
    WHERE s.id IN (34294, 34399, 28305)
  `);
  check.rows.forEach(r => {
    console.log(`${r.street}: geo_id=${r.geo_id} (${r.geo_name}, ${r.type})`);
  });

  await client.end();
}

main().catch(console.error);
