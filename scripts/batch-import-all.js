const { Client } = require('pg');

const localDb = new Client({
  host: 'localhost',
  port: 5433,
  user: 'postgres',
  password: 'postgres',
  database: 'aggregator_dump'
});

const railwayDb = new Client({
  host: 'maglev.proxy.rlwy.net',
  port: 38842,
  user: 'postgres',
  password: 'postgis_valuation_2024',
  database: 'valuation'
});

function toFloat(val) {
  if (val == null) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

function toInt(val) {
  if (val == null) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : Math.floor(num);
}

async function main() {
  await localDb.connect();
  await railwayDb.connect();

  console.log('Starting batch import of ALL objects...');

  const batchSize = 500;
  let offset = 0;
  let imported = 0;
  let skipped = 0;
  let errors = 0;

  while (true) {
    const props = await localDb.query(`
      SELECT DISTINCT ON (source_id)
        source_id, source_type, deal_type, realty_type,
        lat, lng, price, house_number, apartment_number, corps,
        total_area, rooms, floor, total_floors, condition,
        attributes, description, external_url, is_active,
        realty_platform, primary_data, geo_id
      FROM ready_for_export_all
      ORDER BY source_id, geo_id NULLS LAST
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (props.rows.length === 0) break;

    for (const p of props.rows) {
      try {
        await railwayDb.query(`
          INSERT INTO unified_listings (
            source_type, source_id, deal_type, realty_type,
            geo_id, lat, lng, price, house_number, apartment_number, corps,
            total_area, rooms, floor, total_floors, condition,
            attributes, description, external_url, is_active,
            realty_platform, created_at, updated_at, synced_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, NOW(), NOW(), NOW()
          ) ON CONFLICT (source_type, source_id) DO NOTHING
        `, [
          p.source_type,
          p.source_id,
          p.deal_type || 'sell',
          p.realty_type || 'apartment',
          p.geo_id,
          toFloat(p.lat),
          toFloat(p.lng),
          toInt(p.price),
          p.house_number,
          toInt(p.apartment_number),
          p.corps,
          toFloat(p.total_area),
          toInt(p.rooms),
          toInt(p.floor),
          toInt(p.total_floors),
          p.condition,
          JSON.stringify(p.attributes || {}),
          JSON.stringify(p.description || {}),
          p.external_url,
          p.is_active,
          p.realty_platform
        ]);
        imported++;
      } catch (e) {
        errors++;
        if (errors <= 5) console.error('Error for', p.source_id, ':', e.message);
      }
    }

    offset += batchSize;
    if (offset % 10000 === 0) {
      console.log(`Progress: ${offset} / imported: ${imported} / skipped: ${skipped} / errors: ${errors}`);
    }
  }

  console.log(`Done! Imported: ${imported}, Skipped: ${skipped}, Errors: ${errors}`);

  const final = await railwayDb.query('SELECT COUNT(*) as total FROM unified_listings');
  console.log('Total in unified_listings:', final.rows[0].total);

  await localDb.end();
  await railwayDb.end();
}

main().catch(console.error);
