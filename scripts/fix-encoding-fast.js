const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();
  console.log('=== Быстрое исправление кодировки через SQL ===\n');

  // Создаём функцию для исправления кодировки
  await client.query(`
    CREATE OR REPLACE FUNCTION fix_encoding(text) RETURNS text AS $$
    BEGIN
      RETURN convert_from(convert_to($1, 'WIN1251'), 'UTF8');
    EXCEPTION WHEN OTHERS THEN
      RETURN $1;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
  `);
  console.log('Функция fix_encoding создана');

  // Тест функции
  const test = await client.query(`
    SELECT fix_encoding('РІСѓР»РёС†СЏ РџРµС‚СЂР° Р›РµС‰РµРЅРєР°') as fixed
  `);
  console.log('Тест:', test.rows[0].fixed);

  // Обновляем names одним запросом
  console.log('\nОбновление names...');
  const startTime = Date.now();

  const result = await client.query(`
    UPDATE streets
    SET names = jsonb_build_object(
      'uk', (
        SELECT jsonb_agg(fix_encoding(elem))
        FROM jsonb_array_elements_text(names->'uk') AS elem
      ),
      'ru', (
        SELECT COALESCE(jsonb_agg(fix_encoding(elem)), '[]'::jsonb)
        FROM jsonb_array_elements_text(names->'ru') AS elem
      ),
      'en', (
        SELECT COALESCE(jsonb_agg(fix_encoding(elem)), '[]'::jsonb)
        FROM jsonb_array_elements_text(names->'en') AS elem
      )
    )
    WHERE names IS NOT NULL
  `);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Обновлено ${result.rowCount} записей за ${elapsed}s`);

  // Проверка
  console.log('\n--- Проверка результата ---');
  const check = await client.query(`
    SELECT id, name->>'uk' as name_uk, names->'uk' as uk_names
    FROM streets
    WHERE jsonb_array_length(names->'uk') > 1
    LIMIT 5
  `);

  check.rows.forEach(r => {
    console.log(`[${r.id}] ${r.name_uk}`);
    console.log(`  names.uk: ${JSON.stringify(r.uk_names)}`);
  });

  await client.end();
  console.log('\nГотово!');
}

main().catch(console.error);
