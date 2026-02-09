const { Client } = require('pg');
const iconv = require('iconv-lite');

// Попробуем разные варианты декодирования
function tryFix(str) {
  if (!str) return { original: str, fixed: null, method: 'empty' };

  const results = [];

  // Вариант 1: UTF-8 → Windows-1251 → UTF-8
  try {
    const buf = iconv.encode(str, 'utf8');
    const decoded = iconv.decode(buf, 'win1251');
    results.push({ method: 'utf8→win1251', result: decoded });
  } catch (e) {}

  // Вариант 2: Windows-1251 bytes interpreted as UTF-8
  try {
    const buf = iconv.encode(str, 'win1251');
    const decoded = iconv.decode(buf, 'utf8');
    results.push({ method: 'win1251→utf8', result: decoded });
  } catch (e) {}

  // Вариант 3: ISO-8859-1 → UTF-8
  try {
    const buf = Buffer.from(str, 'utf8');
    const asLatin = buf.toString('latin1');
    const buf2 = Buffer.from(asLatin, 'utf8');
    results.push({ method: 'double-utf8', result: buf2.toString('utf8') });
  } catch (e) {}

  // Вариант 4: Encode as win1251 then decode as utf8
  try {
    const buf = iconv.encode(str, 'win1251');
    const decoded = buf.toString('utf8');
    results.push({ method: 'win1251-buf→utf8-str', result: decoded });
  } catch (e) {}

  return results;
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  // Получим тестовую строку
  const result = await client.query(`
    SELECT names->'uk'->0 as name0
    FROM streets
    WHERE id = 28305
  `);

  const broken = result.rows[0].name0;
  console.log('Original broken:', broken);
  console.log('Expected: вулиця Петра Лещенка\n');

  const fixes = tryFix(broken);
  console.log('Попытки исправления:');
  fixes.forEach(f => {
    console.log(`  ${f.method}: ${f.result}`);
  });

  await client.end();
}

main().catch(console.error);
