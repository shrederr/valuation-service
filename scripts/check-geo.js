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

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Найдем Одессу и её районы
  const result = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru, parent_id, lft, rgt, type
    FROM geo
    WHERE name::text ILIKE '%одес%'
    ORDER BY lft
    LIMIT 20
  `);

  console.log('Geo записи с "Одес":\n');
  result.rows.forEach(r => {
    const nameUk = fixEncoding(r.name_uk) || r.name_uk;
    const nameRu = fixEncoding(r.name_ru) || r.name_ru;
    console.log(`ID: ${r.id} | ${nameUk} (${nameRu}) | parent: ${r.parent_id} | type: ${r.type}`);
  });

  // Посмотрим структуру Одеського району
  const odesa = await client.query(`
    SELECT id, name->>'uk' as name_uk, parent_id, type, lft, rgt
    FROM geo
    WHERE id = (SELECT geo_id FROM streets WHERE id = 34294)
  `);

  if (odesa.rows.length > 0) {
    const r = odesa.rows[0];
    console.log(`\nУлица 34294 привязана к geo_id ${r.id}:`);
    console.log(`  name: ${fixEncoding(r.name_uk)}`);
    console.log(`  parent_id: ${r.parent_id}`);
    console.log(`  type: ${r.type}`);

    // Получим родителей
    const parents = await client.query(`
      SELECT id, name->>'uk' as name_uk, type, parent_id
      FROM geo
      WHERE lft <= $1 AND rgt >= $2
      ORDER BY lft
    `, [r.lft, r.rgt]);

    console.log('\nИерархия (от корня):');
    parents.rows.forEach(p => {
      console.log(`  ${p.type}: ${fixEncoding(p.name_uk)} (id=${p.id})`);
    });
  }

  await client.end();
}

main().catch(console.error);
