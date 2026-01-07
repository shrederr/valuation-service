import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing, Topzone, ApartmentComplex } from '@libs/database';
import { SourceType, DealType, RealtyType, MultiLanguageDto } from '@libs/common';

const logger = new Logger('SyncAggregatorBulk');

interface ImportedProperty {
  id: string;
  deal_type: string;
  realty_type: string;
  realty_platform?: string;
  topzone_id?: string;
  complex_id?: string;
  house_number?: string;
  apartment_number?: string;
  corps?: string;
  lat?: string;
  lng?: string;
  price?: string;
  description?: string;
  external_url?: string;
  is_active?: string;
  attributes?: string;
  primary_data?: string;
  created_at?: string;
  updated_at?: string;
  deleted_at?: string;
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

function parseJsonSafe(value: string | object | null | undefined): Record<string, unknown> | undefined {
  if (!value) return undefined;
  if (typeof value === 'object') return value as Record<string, unknown>;
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

// Attribute mappings (shortened for speed)
const CONDITION_TYPE_MAP: Record<number, string> = {
  81: 'Євроремонт', 1144: 'Дизайнерський ремонт', 1145: 'Чудовий стан',
  69: 'Житлове чисте', 100: 'Ремонт не потрібний', 167: 'Хороший стан',
  97: 'Від будівельників вільне планування', 102: 'Будматеріали', 103: 'Будинок що будується',
  169: 'Після будівельників', 787: 'Від будівельників (з обробкою)', 1146: 'Перша здача',
  99: 'Задовільний стан', 96: 'Незавершений ремонт',
  95: 'Дах потрібний ремонт', 106: 'Потріб. капрем. та дах', 107: 'Потріб. космет. рем. та дах',
  108: 'Потріб. тек. рем. та дах', 173: 'Потрібен капітальний ремонт',
  458: 'Потрібен косметичний ремонт', 459: 'Потрібен поточний ремонт',
  98: 'Аварійний', 101: 'Стіни сирі', 476: 'Після повені', 477: 'Після пожежі',
  105: 'Тільки документи', 783: 'Від будівельників (з обробкою)', 800: 'Після будівельників',
};

const PROJECT_TYPE_MAP: Record<number, string> = {
  79: 'Новобуд', 76: 'Моноліт', 4: 'Висотний будинок',
  82: 'Сталінка', 84: 'Чеська', 644: 'Царський (дореволюційний)',
  6: 'Московський', 3: 'Болгарська', 11: 'Харківський', 60: 'Югославський',
  83: 'Хрущовка', 5: 'Малосімейка', 85: 'Гостинка', 7: 'Прихована вітальня',
  9: 'Спецпроект', 10: 'Старий фонд', 8: 'Сотовий',
  86: 'Бельгійська', 723: 'Польська', 724: 'Польський люкс', 725: 'Польський напівлюкс',
  726: 'Австрійська', 727: 'Австрійський люкс', 764: 'Київка',
};

const REALTOR_UA_STATUS_MAP: Record<string, string> = {
  'Євроремонт': 'Євроремонт', 'Від будівельників (з обробкою)': 'Після будівельників',
  'Від будівельників вільне планування': 'Після будівельників', 'Дизайнерський ремонт': 'Євроремонт',
  'Задовільний стан': 'Житлове чисте', 'Незавершений ремонт': 'Потрібен косметичний ремонт',
  'Перша здача': 'Житлове чисте', 'Потрібен капітальний ремонт': 'Потрібен капітальний ремонт',
  'Потрібен косметичний ремонт': 'Потрібен косметичний ремонт', 'Хороший стан': 'Житлове чисте',
  'Чудовий стан': 'Євроремонт', 'З ремонтом': 'Хороший стан', 'Без ремонту': 'Потрібен косметичний ремонт',
  'Частковий ремонт': 'Хороший стан',
};

const REALTOR_UA_BORDER_MAP: Record<string, string> = {
  'Бетонно монолітний': 'Моноліт', 'Газоблок': 'Газобетон', 'Дореволюційний': 'Старий фонд',
  'Сталінка': 'Сталінка', 'Стара панель': 'Хрущовка', 'Стара цегла': 'Сталінка',
  'Типова панель': 'Чеська', 'Українська панель': 'Харківський', 'Українська цегла': 'Спецпроект',
  'Монолітно-каркасна': 'Моноліт', 'Цегляна': 'Спецпроект', 'Утеплена панель': 'Чеська',
  'Панельна': 'Хрущовка', 'Блочна': 'Спецпроект',
};

const OLX_REPAIR_MAP: Record<string, string> = {
  'Євроремонт': 'Євроремонт', 'Після будівельників': 'Після будівельників',
  'Авторський проект': 'Євроремонт', 'Житловий стан': 'Житлове чисте',
  'Косметичний ремонт': 'Хороший стан', 'Під чистову обробку': 'Після будівельників',
  'Аварійний стан': 'Потрібен капітальний ремонт',
};

const OLX_PROPERTY_TYPE_MAP: Record<string, string> = {
  'Житловий фонд від 2021 р.': 'Новобуд', 'Житловий фонд 2011-2020-і': 'Новобуд',
  'Житловий фонд від 2011 р.': 'Новобуд', 'Житловий фонд 2001-2010-і': 'Спецпроект',
  'Чешка': 'Чеська', 'Хрущовка': 'Хрущовка', 'Житловий фонд 80-90-і': 'Спецпроект',
  'Житловий фонд 91-2000-і': 'Спецпроект', 'Сталінка': 'Сталінка', 'Гостинка': 'Гостинка',
  'Совмін': 'Спецпроект', 'Гуртожиток': 'Гостинка', 'Царський будинок': 'Старий фонд',
};

const DOMRIA_CONDITION_MAP: Record<number, string> = {
  507: 'Після будівельників', 508: 'Потрібен капітальний ремонт', 509: 'Потрібен косметичний ремонт',
  510: 'Євроремонт', 511: 'Житлове чисте', 512: 'Хороший стан',
  513: 'Після будівельників', 514: 'Після будівельників', 515: 'Євроремонт',
};

const DOMRIA_WALL_TYPE_MAP: Record<string, string> = {
  'кирпич': 'Спецпроект', 'панель': 'Хрущовка', 'газоблок': 'Газобетон',
  'монолитно-каркасный': 'Моноліт', 'монолит': 'Моноліт', 'газобетон': 'Газобетон',
  'керамический блок': 'Спецпроект', 'монолитно-кирпичный': 'Моноліт', 'пеноблок': 'Газобетон',
  'железобетон': 'Моноліт', 'монолитно-блочный': 'Моноліт', 'монолитный железобетон': 'Моноліт',
  'цегла': 'Спецпроект',
};

function mapCondition(platform: string | undefined, primaryData: Record<string, unknown> | undefined, attributes: Record<string, unknown> | undefined): string | undefined {
  const mainParams = primaryData?.['main_params'] as Record<string, unknown> | undefined;

  if (platform === 'realtorUa' && mainParams?.['status']) {
    return REALTOR_UA_STATUS_MAP[mainParams['status'] as string];
  }
  if (platform === 'olx' && primaryData?.['params']) {
    const params = primaryData['params'] as any[];
    const repair = params?.find((p: any) => p?.key === 'repair')?.value;
    if (repair) return OLX_REPAIR_MAP[repair];
  }
  if (platform === 'domRia' && primaryData?.['characteristics_values']) {
    const chars = primaryData['characteristics_values'] as Record<string, unknown>;
    const id = extractInteger(chars['516']);
    if (id) return DOMRIA_CONDITION_MAP[id];
  }

  const condId = extractInteger(attributes?.condition_type);
  if (condId) return CONDITION_TYPE_MAP[condId];
  return attributes?.condition as string;
}

function mapHouseType(platform: string | undefined, primaryData: Record<string, unknown> | undefined, attributes: Record<string, unknown> | undefined): string | undefined {
  const mainParams = primaryData?.['main_params'] as Record<string, unknown> | undefined;

  if (platform === 'realtorUa' && mainParams?.['border']) {
    return REALTOR_UA_BORDER_MAP[mainParams['border'] as string];
  }
  if (platform === 'olx' && primaryData?.['params']) {
    const params = primaryData['params'] as any[];
    const propType = params?.find((p: any) => p?.key === 'property_type_appartments_sale')?.value;
    if (propType) return OLX_PROPERTY_TYPE_MAP[propType];
  }
  if (platform === 'domRia' && primaryData?.['wall_type']) {
    return DOMRIA_WALL_TYPE_MAP[(primaryData['wall_type'] as string).toLowerCase()];
  }

  const projId = extractInteger(attributes?.project);
  if (projId) return PROJECT_TYPE_MAP[projId];
  return attributes?.houseType as string;
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '5000', 10);
  const startOffset = parseInt(args[1] || '0', 10);

  logger.log('='.repeat(60));
  logger.log('Sync Aggregator BULK (no geo resolution during import)');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Start offset: ${startOffset}`);
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);

  const listingRepository = dataSource.getRepository(UnifiedListing);
  const topzoneRepository = dataSource.getRepository(Topzone);
  const complexRepository = dataSource.getRepository(ApartmentComplex);

  // Check if import table exists
  const tableCheck = await dataSource.query(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'aggregator_import'
    ) as exists
  `);

  if (!tableCheck[0].exists) {
    logger.error('Table aggregator_import does not exist!');
    await app.close();
    process.exit(1);
  }

  // Get total count
  const countResult = await dataSource.query(`
    SELECT COUNT(*) as count FROM aggregator_import
    WHERE lat != '' AND lng != ''
  `);
  const totalCount = parseInt(countResult[0].count, 10);
  logger.log(`Total records with coordinates: ${totalCount}`);

  // Pre-fetch all valid topzone and complex IDs
  logger.log('Pre-fetching topzone and complex IDs...');
  const allTopzones = await topzoneRepository.find({ select: ['id'] });
  const allComplexes = await complexRepository.find({ select: ['id'] });
  const validTopzoneIds = new Set(allTopzones.map(t => t.id));
  const validComplexIds = new Set(allComplexes.map(c => c.id));
  logger.log(`Valid topzones: ${validTopzoneIds.size}, complexes: ${validComplexIds.size}`);

  let totalSynced = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let batchNumber = 0;
  let currentOffset = startOffset;

  const startTime = Date.now();

  try {
    // PHASE 1: Import all data WITHOUT geo resolution
    logger.log('\n>>> PHASE 1: Importing data (no geo resolution)...\n');

    while (true) {
      const properties = await dataSource.query<ImportedProperty[]>(`
        SELECT * FROM aggregator_import
        WHERE lat != '' AND lng != ''
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, currentOffset]);

      if (properties.length === 0) {
        logger.log('No more records to process');
        break;
      }

      batchNumber++;
      const batchStart = Date.now();

      const listingsToUpsert: Partial<UnifiedListing>[] = [];

      for (const item of properties) {
        try {
          const lat = extractNumber(item.lat);
          const lng = extractNumber(item.lng);
          if (!lat || !lng) continue;

          const sourceId = extractInteger(item.id);
          if (!sourceId) continue;

          const attributes = parseJsonSafe(item.attributes);
          const primaryData = parseJsonSafe(item.primary_data);
          const platform = item.realty_platform;

          const topzoneIdNum = extractInteger(item.topzone_id);
          const complexIdNum = extractInteger(item.complex_id);

          const description = parseJsonSafe(item.description);

          listingsToUpsert.push({
            sourceType: SourceType.AGGREGATOR,
            sourceId,
            dealType: mapDealType(item.deal_type),
            realtyType: mapRealtyType(item.realty_type),
            // NO geoId, NO streetId - will be set in Phase 2
            topzoneId: topzoneIdNum && validTopzoneIds.has(topzoneIdNum) ? topzoneIdNum : undefined,
            complexId: complexIdNum && validComplexIds.has(complexIdNum) ? complexIdNum : undefined,
            houseNumber: item.house_number,
            apartmentNumber: item.apartment_number ? parseInt(item.apartment_number, 10) : undefined,
            corps: item.corps,
            lat,
            lng,
            price: extractNumber(item.price),
            currency: 'USD',
            pricePerMeter: (() => {
              const ppm = extractNumber(attributes?.price_sqr ?? attributes?.pricePerMeter);
              if (ppm && ppm > 0) return ppm;
              const price = extractNumber(item.price);
              const area = extractNumber(attributes?.square_total ?? attributes?.totalArea);
              if (price && price > 0 && area && area > 0) return Math.round(price / area);
              return undefined;
            })(),
            totalArea: extractNumber(attributes?.square_total ?? attributes?.totalArea),
            livingArea: extractNumber(attributes?.square_living ?? attributes?.livingArea),
            kitchenArea: extractNumber(attributes?.square_kitchen ?? attributes?.kitchenArea),
            rooms: extractInteger(attributes?.rooms_count ?? attributes?.rooms),
            floor: extractInteger(attributes?.floor),
            totalFloors: extractInteger(attributes?.floors_count ?? attributes?.totalFloors),
            condition: mapCondition(platform, primaryData, attributes),
            houseType: mapHouseType(platform, primaryData, attributes),
            attributes: attributes,
            description: description as unknown as MultiLanguageDto,
            externalUrl: item.external_url,
            isActive: item.is_active === 't' || item.is_active === 'true' || item.is_active === '1',
            publishedAt: item.created_at && item.created_at !== '' ? new Date(item.created_at) : undefined,
            deletedAt: item.deleted_at && item.deleted_at !== '' ? new Date(item.deleted_at) : undefined,
            syncedAt: new Date(),
          });
        } catch (error) {
          totalErrors++;
        }
      }

      // Bulk upsert in chunks (PostgreSQL has ~65535 param limit, ~20 columns = ~3000 records max)
      const UPSERT_CHUNK_SIZE = 500;
      if (listingsToUpsert.length > 0) {
        for (let i = 0; i < listingsToUpsert.length; i += UPSERT_CHUNK_SIZE) {
          const chunk = listingsToUpsert.slice(i, i + UPSERT_CHUNK_SIZE);
          try {
            await listingRepository.upsert(chunk as any, {
              conflictPaths: ['sourceType', 'sourceId'],
              skipUpdateIfNoValuesChanged: true,
            });
            totalSynced += chunk.length;
          } catch (error) {
            logger.error(`Upsert chunk failed: ${(error as Error).message}`);
            totalErrors += chunk.length;
          }
        }
      }

      totalSkipped += properties.length - listingsToUpsert.length;
      currentOffset += batchSize;

      const batchTime = ((Date.now() - batchStart) / 1000).toFixed(1);
      const progress = Math.round((currentOffset / totalCount) * 100);
      const recordsPerSec = Math.round(listingsToUpsert.length / parseFloat(batchTime));

      logger.log(`Batch ${batchNumber}: ${listingsToUpsert.length} records in ${batchTime}s (${recordsPerSec} rec/s) | ${progress}%`);
    }

    const phase1Time = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    logger.log(`\nPhase 1 complete in ${phase1Time} min: synced=${totalSynced}, skipped=${totalSkipped}, errors=${totalErrors}`);

    // PHASE 2: Batch update geoId using spatial join
    logger.log('\n>>> PHASE 2: Resolving geo IDs via spatial join...\n');

    const geoUpdateStart = Date.now();

    // Update geoId for all listings with coordinates using a single spatial query
    const geoUpdateResult = await dataSource.query(`
      UPDATE unified_listings ul
      SET geo_id = g.id
      FROM geo g
      WHERE ul.geo_id IS NULL
        AND ul.lat IS NOT NULL
        AND ul.lng IS NOT NULL
        AND g.polygon IS NOT NULL
        AND ST_Contains(g.polygon, ST_SetSRID(ST_MakePoint(ul.lng, ul.lat), 4326))
    `);

    const geoUpdateTime = ((Date.now() - geoUpdateStart) / 1000).toFixed(1);
    logger.log(`Geo update completed in ${geoUpdateTime}s, affected rows: ${geoUpdateResult[1] || 'unknown'}`);

    // PHASE 3: Batch update streetId using nearest street
    logger.log('\n>>> PHASE 3: Resolving street IDs via nearest street...\n');

    const streetUpdateStart = Date.now();

    // Update streetId using nearest street within 200m
    const streetUpdateResult = await dataSource.query(`
      UPDATE unified_listings ul
      SET street_id = nearest.street_id
      FROM (
        SELECT DISTINCT ON (ul2.id) ul2.id as listing_id, s.id as street_id
        FROM unified_listings ul2
        CROSS JOIN LATERAL (
          SELECT s.id
          FROM streets s
          WHERE s.geo_id = ul2.geo_id
            AND ST_DWithin(
              s.line::geography,
              ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography,
              200
            )
          ORDER BY ST_Distance(
            s.line::geography,
            ST_SetSRID(ST_MakePoint(ul2.lng, ul2.lat), 4326)::geography
          )
          LIMIT 1
        ) s
        WHERE ul2.street_id IS NULL
          AND ul2.geo_id IS NOT NULL
          AND ul2.lat IS NOT NULL
          AND ul2.lng IS NOT NULL
      ) nearest
      WHERE ul.id = nearest.listing_id
    `);

    const streetUpdateTime = ((Date.now() - streetUpdateStart) / 1000).toFixed(1);
    logger.log(`Street update completed in ${streetUpdateTime}s, affected rows: ${streetUpdateResult[1] || 'unknown'}`);

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('\n' + '='.repeat(60));
    logger.log('Processing completed');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Total synced: ${totalSynced}`);
    logger.log(`Total skipped: ${totalSkipped}`);
    logger.log(`Total errors: ${totalErrors}`);
    logger.log('='.repeat(60));
  } finally {
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Processing failed:', error);
  process.exit(1);
});
