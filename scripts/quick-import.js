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

function toInt(val) {
  if (val == null) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : Math.floor(num);
}

function toFloat(val) {
  if (val == null) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

async function main() {
  await aggregatorDb.connect();
  await railwayDb.connect();
  
  console.log('Starting import...');
  
  const batchSize = 500;
  let offset = 0;
  let imported = 0;
  let errors = 0;
  let skipped = 0;
  
  while (true) {
    const props = await aggregatorDb.query(`
      SELECT id, deal_type, realty_type, lat, lng, price, house_number, 
             apartment_number, corps, attributes, description, external_url,
             is_active, realty_platform, primary_data
      FROM exported_properties 
      WHERE is_active = true
      ORDER BY id
      LIMIT $1 OFFSET $2
    `, [batchSize, offset]);
    
    if (props.rows.length === 0) break;
    
    for (const p of props.rows) {
      try {
        const exists = await railwayDb.query(
          'SELECT id FROM unified_listings WHERE source_type = $1 AND source_id = $2',
          ['aggregator', p.id]
        );
        
        if (exists.rows.length > 0) {
          skipped++;
          continue;
        }
        
        const attrs = p.attributes || {};
        
        await railwayDb.query(`
          INSERT INTO unified_listings (
            source_type, source_id, deal_type, realty_type,
            lat, lng, price, house_number, apartment_number, corps,
            total_area, rooms, floor, total_floors, condition, 
            attributes, description, external_url, is_active,
            realty_platform, primary_data, created_at, updated_at, synced_at
          ) VALUES (
            'aggregator', $1, $2, $3,
            $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20, NOW(), NOW(), NOW()
          )
        `, [
          p.id, p.deal_type || 'sell', p.realty_type || 'apartment',
          toFloat(p.lat), toFloat(p.lng), toInt(p.price), p.house_number, toInt(p.apartment_number), p.corps,
          toFloat(attrs.square_total), toInt(attrs.rooms_count), toInt(attrs.floor), toInt(attrs.floors_count), 
          attrs.condition_type ? String(attrs.condition_type) : null,
          JSON.stringify(attrs), JSON.stringify(p.description), p.external_url, p.is_active,
          p.realty_platform, JSON.stringify(p.primary_data)
        ]);
        
        imported++;
      } catch (err) {
        errors++;
        if (errors <= 3) console.error('Error for', p.id, ':', err.message);
      }
    }
    
    offset += batchSize;
    if (offset % 5000 === 0) {
      console.log('Progress:', offset, '/ imported:', imported, '/ skipped:', skipped, '/ errors:', errors);
    }
  }
  
  console.log('Done! Imported:', imported, 'Skipped:', skipped, 'Errors:', errors);
  
  await aggregatorDb.end();
  await railwayDb.end();
}

main().catch(console.error);
