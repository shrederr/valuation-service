const { Client } = require('pg');
const fs = require('fs');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Поиск Дерибасовской
  const deribas = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE name->>'uk' ILIKE '%дериба%' OR name->>'ru' ILIKE '%дериба%'
    LIMIT 5
  `);

  // Поиск Говорова/Добровольців
  const govorova = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE name->>'uk' ILIKE '%говоров%' OR name->>'uk' ILIKE '%добровол%'
       OR names::text ILIKE '%говоров%' OR names::text ILIKE '%добровол%'
    LIMIT 10
  `);

  // Поиск Каманина
  const kamanin = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as names_uk
    FROM streets
    WHERE name->>'uk' ILIKE '%каман%' OR name->>'ru' ILIKE '%каман%'
    LIMIT 5
  `);

  const result = {
    deribas: deribas.rows,
    govorova: govorova.rows,
    kamanin: kamanin.rows
  };

  fs.writeFileSync('output/streets-check.json', JSON.stringify(result, null, 2));
  console.log('Записано в output/streets-check.json');

  await client.end();
}
main().catch(console.error);
