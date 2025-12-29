import { NestFactory } from '@nestjs/core';
import { Module, Logger } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { DatabaseModule, UnifiedListing, Geo, Street, Topzone, ApartmentComplex } from '@libs/database';
import { SourceType, DealType, RealtyType, MultiLanguageDto } from '@libs/common';
import { Repository } from 'typeorm';

const logger = new Logger('SyncProperties');

interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
}

interface AggregatorPropertyDto {
  id: number;
  dealType: string;
  realtyType: string;
  geoId?: number;
  streetId?: number;
  topzoneId?: number;
  complexId?: number;
  houseNumber?: string;
  lat?: number | string;
  lng?: number | string;
  price?: number;
  currency?: string;
  description?: unknown;
  url?: string;
  isActive?: boolean;
  attributes?: Record<string, unknown>;
}

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env.local', '.env'],
    }),
    DatabaseModule,
  ],
})
class SyncPropertiesModule {}

async function fetchFromAggregator<T>(
  baseUrl: string,
  token: string,
  endpoint: string,
  params: Record<string, unknown>,
): Promise<PaginatedResponse<T>> {
  const url = new URL(endpoint, baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.set(key, String(value));
  });

  try {
    const response = await fetch(url.toString(), {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (Array.isArray(data)) {
      return { items: data, total: data.length, page: 1, pageSize: data.length };
    }

    return {
      items: data.items || data.data || [],
      total: data.total || data.count || 0,
      page: data.page || 1,
      pageSize: data.pageSize || data.limit || 100,
    };
  } catch (error) {
    logger.error(`Failed to fetch: ${endpoint}`, error instanceof Error ? error.message : undefined);
    return { items: [], total: 0, page: 1, pageSize: 100 };
  }
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
  const source = args[0] || 'aggregator'; // 'aggregator', 'vector', or 'all'
  const batchSize = parseInt(args[1] || '100', 10);

  logger.log(`Starting property sync from: ${source}`);
  logger.log(`Batch size: ${batchSize}`);

  const app = await NestFactory.createApplicationContext(SyncPropertiesModule);
  const configService = app.get(ConfigService);

  const listingRepository = app.get<Repository<UnifiedListing>>('UnifiedListingRepository');
  const geoRepository = app.get<Repository<Geo>>('GeoRepository');
  const streetRepository = app.get<Repository<Street>>('StreetRepository');
  const topzoneRepository = app.get<Repository<Topzone>>('TopzoneRepository');
  const complexRepository = app.get<Repository<ApartmentComplex>>('ApartmentComplexRepository');

  const aggregatorUrl = configService.get<string>('AGGREGATOR_API_URL');
  const aggregatorToken = configService.get<string>('AGGREGATOR_API_TOKEN');
  const vectorUrl = configService.get<string>('VECTOR_API_URL');
  const vectorToken = configService.get<string>('VECTOR_API_TOKEN');

  if ((source === 'aggregator' || source === 'all') && aggregatorUrl) {
    logger.log('Syncing from aggregator...');
    let page = 1;
    let totalSynced = 0;

    while (true) {
      const response = await fetchFromAggregator<AggregatorPropertyDto>(
        aggregatorUrl,
        aggregatorToken || '',
        'properties/list',
        { page, perPage: batchSize },
      );

      if (!response.items || response.items.length === 0) {
        break;
      }

      for (const item of response.items) {
        try {
          // Validate geo references
          let validGeoId: number | undefined = undefined;
          let validStreetId: number | undefined = undefined;
          let validTopzoneId: number | undefined = undefined;
          let validComplexId: number | undefined = undefined;

          if (item.geoId) {
            const geoExists = await geoRepository.findOne({ where: { id: item.geoId }, select: ['id'] });
            validGeoId = geoExists ? item.geoId : undefined;
          }
          if (item.streetId) {
            const streetExists = await streetRepository.findOne({ where: { id: item.streetId }, select: ['id'] });
            validStreetId = streetExists ? item.streetId : undefined;
          }
          if (item.topzoneId) {
            const topzoneExists = await topzoneRepository.findOne({ where: { id: item.topzoneId }, select: ['id'] });
            validTopzoneId = topzoneExists ? item.topzoneId : undefined;
          }
          if (item.complexId) {
            const complexExists = await complexRepository.findOne({ where: { id: item.complexId }, select: ['id'] });
            validComplexId = complexExists ? item.complexId : undefined;
          }

          const existing = await listingRepository.findOne({
            where: { sourceType: SourceType.AGGREGATOR, sourceId: item.id },
          });

          const listingData = {
            sourceType: SourceType.AGGREGATOR,
            sourceId: item.id,
            dealType: mapDealType(item.dealType),
            realtyType: mapRealtyType(item.realtyType),
            geoId: validGeoId,
            streetId: validStreetId,
            topzoneId: validTopzoneId,
            complexId: validComplexId,
            houseNumber: item.houseNumber,
            lat: extractNumber(item.lat),
            lng: extractNumber(item.lng),
            price: item.price,
            currency: item.currency || 'USD',
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
            externalUrl: item.url,
            isActive: item.isActive ?? true,
            syncedAt: new Date(),
          };

          if (existing) {
            const merged = listingRepository.merge(existing, listingData);
            await listingRepository.save(merged);
          } else {
            await listingRepository.save(listingRepository.create(listingData));
          }
          totalSynced++;
        } catch (error) {
          logger.error(`Failed to sync property ${item.id}: ${(error as Error).message}`);
        }
      }

      logger.log(`Progress: page ${page}, synced ${totalSynced} properties`);

      if (response.items.length < batchSize) {
        break;
      }

      page++;
    }

    logger.log(`Aggregator sync completed: ${totalSynced} properties`);
  }

  if ((source === 'vector' || source === 'all') && vectorUrl) {
    logger.log('Syncing from vector...');
    // Similar logic for vector API
    // ...
  }

  await app.close();
  logger.log('Sync completed');
}

main().catch((error) => {
  logger.error('Sync failed:', error);
  process.exit(1);
});
