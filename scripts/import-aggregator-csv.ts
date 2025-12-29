import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import * as path from 'path';

const logger = new Logger('ImportAggregatorCSV');

async function main() {
  const args = process.argv.slice(2);
  const csvPath = args[0] || path.join(__dirname, '..', 'dumps', 'exported_properties.csv');

  logger.log('='.repeat(60));
  logger.log('Import Aggregator CSV to Local Table');
  logger.log('='.repeat(60));
  logger.log(`CSV file: ${csvPath}`);
  logger.log('='.repeat(60));

  // Create NestJS app context
  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);

  try {
    // Drop existing table if exists
    logger.log('Dropping existing aggregator_import table if exists...');
    await dataSource.query(`DROP TABLE IF EXISTS aggregator_import`);

    // Create import table with TEXT for all columns (CSV may have empty strings)
    logger.log('Creating aggregator_import table...');
    await dataSource.query(`
      CREATE TABLE aggregator_import (
        id TEXT,
        deal_type TEXT,
        realty_type TEXT,
        geo_id TEXT,
        street_id TEXT,
        complex_id TEXT,
        source_id TEXT,
        is_exclusive TEXT,
        house_number TEXT,
        house_number_add TEXT,
        street_mark TEXT,
        lng TEXT,
        lat TEXT,
        images TEXT,
        cadastal_number TEXT,
        attributes TEXT,
        description TEXT,
        published_at TEXT,
        realty_platform TEXT,
        primary_data TEXT,
        corps TEXT,
        external_id TEXT,
        apartment_number TEXT,
        is_agency TEXT,
        is_active TEXT,
        is_exported TEXT,
        seller TEXT,
        external_url TEXT,
        realtor TEXT,
        topzone_id TEXT,
        hash TEXT,
        is_fake_agency TEXT,
        price TEXT,
        created_at TEXT,
        updated_at TEXT,
        deleted_at TEXT,
        other_plarform_attributes TEXT,
        external_param TEXT
      )
    `);

    // Import CSV using mounted volume (./dumps -> /dumps in container)
    logger.log('Importing CSV data via mounted volume...');
    const { execSync } = await import('child_process');

    // Use the mounted path - dumps folder is mounted at /dumps in container
    const containerCsvPath = '/dumps/exported_properties.csv';

    // Run COPY command inside container using mounted file
    logger.log(`Running COPY from ${containerCsvPath}...`);
    const copyCommand = `docker exec valuation-postgres psql -U postgres -d valuation -c "COPY aggregator_import (id, deal_type, realty_type, geo_id, street_id, complex_id, source_id, is_exclusive, house_number, house_number_add, street_mark, lng, lat, images, cadastal_number, attributes, description, published_at, realty_platform, primary_data, corps, external_id, apartment_number, is_agency, is_active, is_exported, seller, external_url, realtor, topzone_id, hash, is_fake_agency, price, created_at, updated_at, deleted_at, other_plarform_attributes, external_param) FROM '${containerCsvPath}' WITH (FORMAT CSV, HEADER true, NULL 'null')"`;

    execSync(copyCommand, {
      stdio: 'inherit',
    });

    // Get count
    const countResult = await dataSource.query(`SELECT COUNT(*) as count FROM aggregator_import`);
    const totalCount = parseInt(countResult[0].count, 10);

    // Create indexes for faster processing
    logger.log('Creating indexes...');
    await dataSource.query(`CREATE INDEX idx_aggregator_import_id ON aggregator_import(id)`);
    await dataSource.query(`CREATE INDEX idx_aggregator_import_is_active ON aggregator_import(is_active)`);
    await dataSource.query(`CREATE INDEX idx_aggregator_import_coords ON aggregator_import(lat, lng) WHERE lat IS NOT NULL AND lng IS NOT NULL`);

    logger.log('='.repeat(60));
    logger.log('Import completed successfully!');
    logger.log(`Total records imported: ${totalCount}`);
    logger.log('='.repeat(60));
    logger.log('');
    logger.log('Now you can run the processing script:');
    logger.log('  yarn ts-node scripts/process-aggregator-dump.ts [batchSize] [startOffset] [maxBatches]');
    logger.log('');
    logger.log('Examples:');
    logger.log('  yarn ts-node scripts/process-aggregator-dump.ts 1000 0 0    # Process all');
    logger.log('  yarn ts-node scripts/process-aggregator-dump.ts 500 0 10    # Process 10 batches of 500');
    logger.log('');
  } catch (error) {
    logger.error(`Import failed: ${(error as Error).message}`);
    throw error;
  } finally {
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Import failed:', error);
  process.exit(1);
});
