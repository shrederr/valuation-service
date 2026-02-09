const { Client } = require('pg');

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });
  await client.connect();

  // Проверим сколько записей с нормальной кодировкой (содержат українські букви)
  const stats = await client.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE names->'uk'->>0 ~ '[іїєґ]') as has_ukr_chars,
      COUNT(*) FILTER (WHERE names->'uk'->>0 ~ '[а-яА-Я]' AND NOT names->'uk'->>0 ~ '[іїєґ]') as ru_only,
      COUNT(*) FILTER (WHERE names->'uk'->>0 ~ 'Р[А-Яа-я]') as broken_encoding,
      COUNT(*) FILTER (WHERE names->'uk'->>0 ~ '�') as question_marks
    FROM streets
    WHERE names IS NOT NULL AND jsonb_array_length(names->'uk') > 0
  `);

  console.log('Статистика кодировки names:');
  console.log('Всего:', stats.rows[0].total);
  console.log('С укр. буквами (і,ї,є,ґ):', stats.rows[0].has_ukr_chars);
  console.log('Только рус. буквы:', stats.rows[0].ru_only);
  console.log('Битая кодировка (Р...):', stats.rows[0].broken_encoding);
  console.log('Знаки вопроса:', stats.rows[0].question_marks);

  await client.end();
}
main().catch(console.error);
