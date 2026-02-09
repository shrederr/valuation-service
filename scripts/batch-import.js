const { Client } = require('pg');

const aggregatorDb = new Client({
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

async function main() {
  await aggregatorDb.connect();
  await railwayDb.connect();

  console.log('Starting batch import from ready_for_export...');

  // Get already imported IDs
  const existingRes = await railwayDb.query(
    "SELECT source_id FROM unified_listings WHERE source_type = 'aggregator'"
  );
  const existingIds = new Set(existingRes.rows.map(r => r.source_id));
  console.log('Already imported:', existingIds.size);

  const batchSize = 1000;
  let offset = 0;
  let imported = 0;
  let skipped = 0;
  let errors = 0;

  while (true) {
    const props = await aggregatorDb.query(`
      SELECT source_id, source_type, deal_type, realty_type,
             lat, lng, price, house_number, apartment_number, corps,
             total_area, rooms, floor, total_floors, condition,
             attributes, description, external_url, is_active,
             realty_platform, primary_data, geo_id
      FROM ready_for_export
      ORDER BY source_id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);

    if (props.rows.length === 0) break;

    // Filter out already imported
    const toImport = props.rows.filter(p => !existingIds.has(p.source_id));
    skipped += (props.rows.length - toImport.length);

    if (toImport.length > 0) {
      // Build batch insert
      const values = [];
      const placeholders = [];
      let paramIndex = 1;

      for (const p of toImport) {
        try {
          placeholders.push(`(
            $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++},
            $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++},
            $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++},
            $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++},
            $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++},
            $${paramIndex++}, NOW(), NOW(), NOW()
          )`);

          values.push(
            p.source_type,
            p.source_id,
            p.deal_type || 'sell',
            p.realty_type || 'apartment',
            p.geo_id,
            p.lat ? parseFloat(p.lat) : null,
            p.lng ? parseFloat(p.lng) : null,
            p.price ? parseInt(p.price) : null,
            p.house_number,
            p.apartment_number ? parseInt(p.apartment_number) : null,
            p.corps,
            p.total_area ? parseFloat(p.total_area) : null,
            p.rooms ? parseInt(p.rooms) : null,
            p.floor ? parseInt(p.floor) : null,
            p.total_floors ? parseInt(p.total_floors) : null,
            p.condition,
            JSON.stringify(p.attributes || {}),
            JSON.stringify(p.description || {}),
            p.external_url,
            p.is_active,
            p.realty_platform
          );
        } catch (e) {
          errors++;
          if (errors <= 3) console.error('Parse error for', p.source_id, ':', e.message);
        }
      }

      if (placeholders.length > 0) {
        try {
          await railwayDb.query(`
            INSERT INTO unified_listings (
              source_type, source_id, deal_type, realty_type,
              geo_id, lat, lng, price, house_number, apartment_number, corps,
              total_area, rooms, floor, total_floors, condition,
              attributes, description, external_url, is_active,
              realty_platform, created_at, updated_at, synced_at
            ) VALUES ${placeholders.join(', ')}
            ON CONFLICT (source_type, source_id) DO NOTHING
          `, values);

          imported += placeholders.length;
        } catch (e) {
          errors += placeholders.length;
          console.error('Batch insert error at offset', offset, ':', e.message);

          // Try one by one for this batch
          for (const p of toImport) {
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
                p.lat ? parseFloat(p.lat) : null,
                p.lng ? parseFloat(p.lng) : null,
                p.price ? parseInt(p.price) : null,
                p.house_number,
                p.apartment_number ? parseInt(p.apartment_number) : null,
                p.corps,
                p.total_area ? parseFloat(p.total_area) : null,
                p.rooms ? parseInt(p.rooms) : null,
                p.floor ? parseInt(p.floor) : null,
                p.total_floors ? parseInt(p.total_floors) : null,
                p.condition,
                JSON.stringify(p.attributes || {}),
                JSON.stringify(p.description || {}),
                p.external_url,
                p.is_active,
                p.realty_platform
              ]);
              imported++;
              errors--; // We counted this as error before
            } catch (e2) {
              if (errors <= 10) console.error('Single insert error for', p.source_id, ':', e2.message);
            }
          }
        }
      }
    }

    offset += batchSize;
    if (offset % 10000 === 0) {
      console.log('Progress:', offset, '/ imported:', imported, '/ skipped:', skipped, '/ errors:', errors);
    }
  }

  console.log('Done! Imported:', imported, 'Skipped:', skipped, 'Errors:', errors);

  // Final count
  const finalCount = await railwayDb.query('SELECT COUNT(*) as total FROM unified_listings');
  console.log('Total in unified_listings:', finalCount.rows[0].total);

  await aggregatorDb.end();
  await railwayDb.end();
}

main().catch(console.error);
