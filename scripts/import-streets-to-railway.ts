import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { Street } from '@libs/database';

const logger = new Logger('ImportStreetsToRailway');

// Local database connection (Docker)
const LOCAL_DB_HOST = process.env.LOCAL_DB_HOST || 'localhost';
const LOCAL_DB_PORT = parseInt(process.env.LOCAL_DB_PORT || '5433', 10);
const LOCAL_DB_USERNAME = process.env.LOCAL_DB_USERNAME || 'postgres';
const LOCAL_DB_PASSWORD = process.env.LOCAL_DB_PASSWORD || 'postgres';
const LOCAL_DB_DATABASE = process.env.LOCAL_DB_DATABASE || 'valuation';

async function main() {
  logger.log('='.repeat(60));
  logger.log('Import Streets to Railway');
  logger.log('='.repeat(60));
  logger.log(`Local DB: ${LOCAL_DB_HOST}:${LOCAL_DB_PORT}/${LOCAL_DB_DATABASE}`);
  logger.log('='.repeat(60));

  // Create connection to local database
  const localDataSource = new DataSource({
    type: 'postgres',
    host: LOCAL_DB_HOST,
    port: LOCAL_DB_PORT,
    username: LOCAL_DB_USERNAME,
    password: LOCAL_DB_PASSWORD,
    database: LOCAL_DB_DATABASE,
  });

  try {
    await localDataSource.initialize();
    logger.log('Connected to local database');
  } catch (error) {
    logger.error(`Failed to connect to local database: ${(error as Error).message}`);
    process.exit(1);
  }

  // Create NestJS app context for Railway database
  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const railwayDataSource = app.get(DataSource);
  const streetRepository = railwayDataSource.getRepository(Street);

  try {
    // Get count from local
    const countResult = await localDataSource.query('SELECT COUNT(*) as count FROM streets');
    const totalCount = parseInt(countResult[0].count, 10);
    logger.log(`Total streets to import: ${totalCount}`);

    // Fetch streets in batches
    const batchSize = 1000;
    let imported = 0;
    let offset = 0;

    while (offset < totalCount) {
      const streets = await localDataSource.query(`
        SELECT id, geo_id, name, osm_name, line, created_at, updated_at
        FROM streets
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, offset]);

      // Insert streets directly using query builder for better performance
      if (streets.length > 0) {
        // Use raw insert for geometry column
        for (const street of streets) {
          try {
            await railwayDataSource.query(`
              INSERT INTO streets (id, geo_id, name, osm_name, line, created_at, updated_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7)
              ON CONFLICT (id) DO UPDATE SET
                geo_id = EXCLUDED.geo_id,
                name = EXCLUDED.name,
                osm_name = EXCLUDED.osm_name,
                line = EXCLUDED.line,
                updated_at = EXCLUDED.updated_at
            `, [
              street.id,
              street.geo_id,
              street.name,
              street.osm_name,
              street.line,
              street.created_at || new Date(),
              street.updated_at || new Date(),
            ]);
            imported++;
          } catch (error) {
            logger.warn(`Failed to import street ${street.id}: ${(error as Error).message}`);
          }
        }
      }

      offset += batchSize;
      logger.log(`Progress: ${Math.min(offset, totalCount)}/${totalCount}`);
    }

    logger.log('='.repeat(60));
    logger.log(`Import completed: ${imported} streets imported`);
    logger.log('='.repeat(60));
  } finally {
    await localDataSource.destroy();
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Import failed:', error);
  process.exit(1);
});
