const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const { Readable } = require('stream');
const copyFrom = require('pg-copy-streams').from;
const copyTo = require('pg-copy-streams').to;

const LOCAL_DB = {
  host: 'localhost',
  port: 5433,
  user: 'postgres',
  password: 'postgres',
  database: 'aggregator_dump'
};

const RAILWAY_DB = {
  host: 'maglev.proxy.rlwy.net',
  port: 38842,
  user: 'postgres',
  password: 'postgis_valuation_2024',
  database: 'valuation'
};

async function main() {
  console.log('=== DUMP-IMPORT APPROACH ===\n');

  const csvPath = path.join(__dirname, 'export_data.csv');

  // Step 1: Export to CSV from local using pg-copy-streams
  console.log('Step 1: Exporting data from local DB to CSV...');

  const localDb = new Client(LOCAL_DB);
  await localDb.connect();

  const exportQuery = `
    COPY (
      SELECT DISTINCT ON (source_id)
        source_type,
        source_id,
        COALESCE(deal_type, 'sell') as deal_type,
        COALESCE(realty_type, 'apartment') as realty_type,
        geo_id,
        lat,
        lng,
        CASE WHEN price::bigint > 2147483647 THEN NULL ELSE price::int END as price,
        house_number,
        CASE WHEN apartment_number::numeric > 2147483647 THEN NULL ELSE FLOOR(apartment_number::numeric)::int END as apartment_number,
        corps,
        total_area::numeric,
        CASE WHEN rooms::numeric > 2147483647 THEN NULL ELSE FLOOR(rooms::numeric)::int END as rooms,
        CASE WHEN floor::numeric > 2147483647 THEN NULL ELSE FLOOR(floor::numeric)::int END as floor,
        CASE WHEN total_floors::numeric > 2147483647 THEN NULL ELSE FLOOR(total_floors::numeric)::int END as total_floors,
        condition,
        attributes::text,
        description::text,
        external_url,
        is_active,
        realty_platform
      FROM ready_for_export_all
      ORDER BY source_id, geo_id NULLS LAST
    ) TO STDOUT WITH (FORMAT CSV, HEADER true)
  `;

  const writeStream = fs.createWriteStream(csvPath);
  const copyStream = localDb.query(copyTo(exportQuery));

  await new Promise((resolve, reject) => {
    copyStream.on('error', reject);
    writeStream.on('error', reject);
    writeStream.on('finish', resolve);
    copyStream.pipe(writeStream);
  });

  console.log('CSV export completed!');
  await localDb.end();

  // Check CSV size
  const stats = fs.statSync(csvPath);
  console.log(`CSV file size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);

  // Step 2: Create temp table on Railway and import
  console.log('\nStep 2: Creating temp table on Railway...');

  const railwayDb = new Client(RAILWAY_DB);
  await railwayDb.connect();

  // Create temp table
  await railwayDb.query(`
    DROP TABLE IF EXISTS temp_import;
    CREATE TABLE temp_import (
      source_type varchar,
      source_id int,
      deal_type varchar,
      realty_type varchar,
      geo_id int,
      lat numeric,
      lng numeric,
      price int,
      house_number varchar,
      apartment_number int,
      corps varchar,
      total_area numeric,
      rooms int,
      floor int,
      total_floors int,
      condition varchar,
      attributes text,
      description text,
      external_url varchar,
      is_active boolean,
      realty_platform varchar
    )
  `);
  console.log('Temp table created.');

  // Import via COPY FROM
  console.log('Importing CSV to Railway temp_import...');
  const importQuery = `COPY temp_import FROM STDIN WITH (FORMAT CSV, HEADER true)`;
  const readStream = fs.createReadStream(csvPath);
  const copyInStream = railwayDb.query(copyFrom(importQuery));

  await new Promise((resolve, reject) => {
    copyInStream.on('error', reject);
    copyInStream.on('finish', resolve);
    readStream.on('error', reject);
    readStream.pipe(copyInStream);
  });

  console.log('CSV import completed!');

  // Check temp table count
  const tempCount = await railwayDb.query('SELECT COUNT(*) FROM temp_import');
  console.log(`Records in temp_import: ${tempCount.rows[0].count}`);

  // Step 3: Insert from temp to unified_listings
  console.log('\nStep 3: Inserting from temp_import to unified_listings...');

  const insertResult = await railwayDb.query(`
    INSERT INTO unified_listings (
      source_type, source_id, deal_type, realty_type,
      geo_id, lat, lng, price, house_number, apartment_number, corps,
      total_area, rooms, floor, total_floors, condition,
      attributes, description, external_url, is_active,
      realty_platform, created_at, updated_at, synced_at
    )
    SELECT
      source_type::unified_listings_source_type_enum, source_id, deal_type::unified_listings_deal_type_enum, realty_type::unified_listings_realty_type_enum,
      geo_id, lat, lng, price, house_number, apartment_number, corps,
      total_area, rooms, floor, total_floors, condition,
      CASE WHEN attributes IS NOT NULL AND attributes != '' THEN attributes::jsonb ELSE '{}'::jsonb END,
      CASE WHEN description IS NOT NULL AND description != '' THEN description::jsonb ELSE '{}'::jsonb END,
      external_url, is_active,
      realty_platform, NOW(), NOW(), NOW()
    FROM temp_import
    ON CONFLICT (source_type, source_id) DO NOTHING
  `);
  console.log(`Inserted ${insertResult.rowCount} new records.`);

  // Final count
  const finalCount = await railwayDb.query('SELECT COUNT(*) FROM unified_listings');
  console.log(`\nTotal in unified_listings: ${finalCount.rows[0].count}`);

  // Cleanup
  await railwayDb.query('DROP TABLE IF EXISTS temp_import');
  console.log('Temp table dropped.');

  await railwayDb.end();

  // Remove CSV
  fs.unlinkSync(csvPath);
  console.log('CSV file removed.');

  console.log('\n=== DONE ===');
}

main().catch(console.error);
