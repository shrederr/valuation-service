import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing, Geo, Street, Topzone, ApartmentComplex } from '@libs/database';
import { SourceType, DealType, RealtyType, MultiLanguageDto } from '@libs/common';
import { GeoLookupService } from '../apps/valuation/src/modules/osm/geo-lookup.service';

const logger = new Logger('SyncFromAggregatorDB');

// Aggregator database connection settings - configure via env or args
const AGGREGATOR_DB_HOST = process.env.AGGREGATOR_DB_HOST || 'localhost';
const AGGREGATOR_DB_PORT = parseInt(process.env.AGGREGATOR_DB_PORT || '5432', 10);
const AGGREGATOR_DB_USERNAME = process.env.AGGREGATOR_DB_USERNAME || 'postgres';
const AGGREGATOR_DB_PASSWORD = process.env.AGGREGATOR_DB_PASSWORD || 'postgres';
const AGGREGATOR_DB_DATABASE = process.env.AGGREGATOR_DB_DATABASE || 'aggregator';

interface AggregatorProperty {
  id: number;
  deal_type: string;
  realty_type: string;
  geo_id?: number;
  street_id?: number;
  topzone_id?: number;
  complex_id?: number;
  house_number?: string;
  apartment_number?: number;
  corps?: string;
  lat?: string;
  lng?: string;
  price?: number;
  description?: Record<string, string>;
  external_url?: string;
  is_active?: boolean;
  attributes?: Record<string, unknown>;
  created_at?: Date;
  updated_at?: Date;
}

function mapDealType(dealType: string): DealType {
  const normalized = dealType?.toLowerCase();
  if (normalized === 'rent') return DealType.Rent;
  if (normalized === 'sell' || normalized === 'buy' || normalized === 'sale') return DealType.Sell;
  return DealType.Sell;
}

function mapRealtyType(realtyType: string): RealtyType {
  const normalized = realtyType?.toLowerCase();
  if (normalized === 'apartment' || normalized === 'flat') return RealtyType.Apartment;
  if (normalized === 'house') return RealtyType.House;
  if (normalized === 'commercial') return RealtyType.Commercial;
  if (normalized === 'land' || normalized === 'area') return RealtyType.Area;
  if (normalized === 'garage') return RealtyType.Garage;
  if (normalized === 'room') return RealtyType.Room;
  return RealtyType.Apartment;
}

function extractNumber(value: unknown): number | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'number') return isNaN(value) ? undefined : value;
  if (typeof value === 'string') {
    const num = parseFloat(value);
    return isNaN(num) ? undefined : num;
  }
  return undefined;
}

function extractInteger(value: unknown): number | undefined {
  const num = extractNumber(value);
  if (num === undefined) return undefined;
  return Math.floor(num);
}

async function main() {
  const args = process.argv.slice(2);
  const limit = parseInt(args[0] || '1000', 10);
  const offset = parseInt(args[1] || '0', 10);

  logger.log('='.repeat(60));
  logger.log('Sync from Aggregator Database');
  logger.log('='.repeat(60));
  logger.log(`Aggregator DB: ${AGGREGATOR_DB_HOST}:${AGGREGATOR_DB_PORT}/${AGGREGATOR_DB_DATABASE}`);
  logger.log(`Limit: ${limit}, Offset: ${offset}`);
  logger.log('='.repeat(60));

  // Create connection to aggregator database
  const aggregatorDataSource = new DataSource({
    type: 'postgres',
    host: AGGREGATOR_DB_HOST,
    port: AGGREGATOR_DB_PORT,
    username: AGGREGATOR_DB_USERNAME,
    password: AGGREGATOR_DB_PASSWORD,
    database: AGGREGATOR_DB_DATABASE,
  });

  try {
    await aggregatorDataSource.initialize();
    logger.log('Connected to aggregator database');
  } catch (error) {
    logger.error(`Failed to connect to aggregator database: ${(error as Error).message}`);
    process.exit(1);
  }

  // Create NestJS app context for our database
  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const valuationDataSource = app.get(DataSource);
  const geoLookupService = app.get(GeoLookupService);

  const listingRepository = valuationDataSource.getRepository(UnifiedListing);
  const geoRepository = valuationDataSource.getRepository(Geo);
  const streetRepository = valuationDataSource.getRepository(Street);
  const topzoneRepository = valuationDataSource.getRepository(Topzone);
  const complexRepository = valuationDataSource.getRepository(ApartmentComplex);

  let geoResolvedByCoords = 0;
  let streetResolvedByCoords = 0;

  try {
    // Query properties from aggregator database
    // Try different table names that might exist
    const tableNames = ['exported_properties', 'exported_property', 'properties', 'property'];
    let tableName = '';

    for (const name of tableNames) {
      try {
        await aggregatorDataSource.query(`SELECT 1 FROM ${name} LIMIT 1`);
        tableName = name;
        logger.log(`Found table: ${name}`);
        break;
      } catch {
        // Table doesn't exist, try next
      }
    }

    if (!tableName) {
      logger.error('Could not find properties table in aggregator database');
      logger.log('Available tables:');
      const tables = await aggregatorDataSource.query(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
      `);
      tables.forEach((t: { table_name: string }) => logger.log(`  - ${t.table_name}`));
      process.exit(1);
    }

    // Find all geo_ids within Odessa CITY using Nested Set (lft/rgt)
    // In aggregator: geo_id=2 is "Одеса" (city) - we sync only the city and its districts
    const odessaGeoIds = await aggregatorDataSource.query(`
      SELECT child.id FROM geo child
      INNER JOIN geo parent ON child.lft >= parent.lft AND child.rgt <= parent.rgt
      WHERE parent.id = 2 AND parent.type = 'city'
    `);
    const geoIdList = odessaGeoIds.map((g: { id: number }) => g.id);
    logger.log(`Found ${geoIdList.length} geo_ids within Odessa city`);

    // Get count of properties - either by geo_id filter or all active
    const countResult = geoIdList.length > 0
      ? await aggregatorDataSource.query(`
          SELECT COUNT(*) as count FROM ${tableName}
          WHERE is_active = true AND geo_id = ANY($1)
        `, [geoIdList])
      : await aggregatorDataSource.query(`
          SELECT COUNT(*) as count FROM ${tableName}
          WHERE is_active = true
        `);
    const totalCount = parseInt(countResult[0].count, 10);
    logger.log(`Total properties to sync: ${totalCount}`);

    // Fetch properties - if we found Odessa geo_ids, filter by them
    const properties = geoIdList.length > 0
      ? await aggregatorDataSource.query<AggregatorProperty[]>(`
          SELECT * FROM ${tableName}
          WHERE is_active = true AND geo_id = ANY($1)
          ORDER BY id
          LIMIT $2 OFFSET $3
        `, [geoIdList, limit, offset])
      : await aggregatorDataSource.query<AggregatorProperty[]>(`
          SELECT * FROM ${tableName}
          WHERE is_active = true
          ORDER BY id
          LIMIT $1 OFFSET $2
        `, [limit, offset]);

    logger.log(`Fetched ${properties.length} properties`);

    let synced = 0;
    let skipped = 0;
    let errors = 0;

    for (const item of properties) {
      try {
        // IMPORTANT: Do NOT use geo_id/street_id from aggregator - IDs don't match between databases!
        // Always resolve geo by coordinates using our GeoLookupService
        let validGeoId: number | undefined = undefined;
        let validStreetId: number | undefined = undefined;
        let validTopzoneId: number | undefined = undefined;
        let validComplexId: number | undefined = undefined;

        // Topzone and complex IDs might match if synced from same source
        if (item.topzone_id) {
          const topzoneExists = await topzoneRepository.findOne({ where: { id: item.topzone_id }, select: ['id'] });
          validTopzoneId = topzoneExists ? item.topzone_id : undefined;
        }
        if (item.complex_id) {
          const complexExists = await complexRepository.findOne({ where: { id: item.complex_id }, select: ['id'] });
          validComplexId = complexExists ? item.complex_id : undefined;
        }

        // Always resolve geo and street by coordinates
        const lat = extractNumber(item.lat);
        const lng = extractNumber(item.lng);

        if (lat && lng) {
          const resolved = await geoLookupService.resolveGeoForListing(lng, lat);
          if (resolved.geoId) {
            validGeoId = resolved.geoId;
            geoResolvedByCoords++;
          }
          if (resolved.streetId) {
            validStreetId = resolved.streetId;
            streetResolvedByCoords++;
          }
        }

        const existing = await listingRepository.findOne({
          where: { sourceType: SourceType.AGGREGATOR, sourceId: item.id },
        });

        const listingData: Partial<UnifiedListing> = {
          sourceType: SourceType.AGGREGATOR,
          sourceId: item.id,
          dealType: mapDealType(item.deal_type),
          realtyType: mapRealtyType(item.realty_type),
          geoId: validGeoId,
          streetId: validStreetId,
          topzoneId: validTopzoneId,
          complexId: validComplexId,
          houseNumber: item.house_number,
          apartmentNumber: item.apartment_number,
          corps: item.corps,
          lat: extractNumber(item.lat),
          lng: extractNumber(item.lng),
          price: item.price,
          currency: 'USD',
          pricePerMeter: extractNumber(item.attributes?.price_sqr ?? item.attributes?.pricePerMeter),
          totalArea: extractNumber(item.attributes?.square_total ?? item.attributes?.totalArea),
          livingArea: extractNumber(item.attributes?.square_living ?? item.attributes?.livingArea),
          kitchenArea: extractNumber(item.attributes?.square_kitchen ?? item.attributes?.kitchenArea),
          rooms: extractInteger(item.attributes?.rooms_count ?? item.attributes?.rooms),
          floor: extractInteger(item.attributes?.floor),
          totalFloors: extractInteger(item.attributes?.floors_count ?? item.attributes?.totalFloors),
          condition: item.attributes?.condition as string,
          houseType: item.attributes?.houseType as string,
          attributes: item.attributes,
          description: item.description as unknown as MultiLanguageDto,
          externalUrl: item.external_url,
          isActive: item.is_active ?? true,
          syncedAt: new Date(),
        };

        if (existing) {
          const merged = listingRepository.merge(existing, listingData);
          await listingRepository.save(merged);
        } else {
          await listingRepository.save(listingRepository.create(listingData));
        }
        synced++;

        if (synced % 100 === 0) {
          logger.log(`Progress: ${synced}/${properties.length}`);
        }
      } catch (error) {
        errors++;
        logger.error(`Failed to sync property ${item.id}: ${(error as Error).message}`);
      }
    }

    logger.log('='.repeat(60));
    logger.log('Sync completed');
    logger.log(`Synced: ${synced}, Skipped: ${skipped}, Errors: ${errors}`);
    logger.log(`Geo resolved by coordinates: ${geoResolvedByCoords}`);
    logger.log(`Streets resolved by coordinates: ${streetResolvedByCoords}`);
    logger.log('='.repeat(60));
  } finally {
    await aggregatorDataSource.destroy();
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Sync failed:', error);
  process.exit(1);
});
