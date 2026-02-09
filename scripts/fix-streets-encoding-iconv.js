const { Client } = require('pg');
const iconv = require('iconv-lite');

// Fix double-encoded text: UTF-8 → read as CP1251 → stored as UTF-8
// We need to reverse: take UTF-8 string → interpret chars as CP1251 bytes → decode as UTF-8
function fixDoubleEncoding(str) {
  if (!str) return str;
  try {
    // Get the code points of each character
    // Treat them as byte values (CP1251)
    const bytes = [];
    for (let i = 0; i < str.length; i++) {
      const code = str.charCodeAt(i);
      if (code < 256) {
        bytes.push(code);
      } else {
        // Character outside Latin1 range - try to get its Windows-1251 byte
        // This is tricky...
        bytes.push(code & 0xFF);
      }
    }
    // Decode as UTF-8
    const buffer = Buffer.from(bytes);
    return buffer.toString('utf8');
  } catch (e) {
    return str;
  }
}

// Alternative: use iconv to encode as CP1251 then decode as UTF-8
function fixWithIconv(str) {
  if (!str) return str;
  try {
    // Encode the string to CP1251 bytes (treating characters as CP1251)
    const cp1251Buffer = iconv.encode(str, 'win1251');
    // Decode those bytes as UTF-8
    const fixed = iconv.decode(cp1251Buffer, 'utf8');
    return fixed;
  } catch (e) {
    return str;
  }
}

async function main() {
  const client = new Client({
    connectionString: 'postgresql://postgres:postgis_valuation_2024@maglev.proxy.rlwy.net:38842/valuation'
  });

  await client.connect();

  const samples = await client.query(`
    SELECT id, name->>'uk' as name_uk, name->>'ru' as name_ru
    FROM streets
    WHERE osm_id IS NOT NULL
    LIMIT 10
  `);

  console.log('Testing encoding fix with iconv:\n');
  for (const s of samples.rows) {
    console.log('Original UK:', s.name_uk);
    console.log('Fixed (method 1):', fixDoubleEncoding(s.name_uk));
    console.log('Fixed (iconv):', fixWithIconv(s.name_uk));
    console.log('---');
  }

  await client.end();
}

main().catch(console.error);
