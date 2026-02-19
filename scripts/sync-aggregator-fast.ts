import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing, Topzone, ApartmentComplex } from '@libs/database';
import { SourceType, DealType, RealtyType, MultiLanguageDto } from '@libs/common';
import { GeoLookupService } from '../apps/valuation/src/modules/osm/geo-lookup.service';

const logger = new Logger('SyncAggregatorFast');

interface ImportedProperty {
  id: string;
  deal_type: string;
  realty_type: string;
  realty_platform?: string;
  geo_id?: string;
  street_id?: string;
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

// ============================================================
// Attribute Mappings from vector-api
// ============================================================

const CONDITION_TYPE_MAP: Record<number, string> = {
  81: 'Євроремонт',
  1144: 'Дизайнерський ремонт',
  1145: 'Чудовий стан',
  69: 'Житлове чисте',
  100: 'Ремонт не потрібний',
  167: 'Хороший стан',
  97: 'Від будівельників вільне планування',
  102: 'Будматеріали',
  103: 'Будинок що будується',
  169: 'Після будівельників',
  787: 'Від будівельників (з обробкою)',
  1146: 'Перша здача',
  99: 'Задовільний стан',
  96: 'Незавершений ремонт',
  95: 'Дах потрібний ремонт',
  106: 'Потріб. капрем. та дах',
  107: 'Потріб. космет. рем. та дах',
  108: 'Потріб. тек. рем. та дах',
  173: 'Потрібен капітальний ремонт',
  458: 'Потрібен косметичний ремонт',
  459: 'Потрібен поточний ремонт',
  98: 'Аварійний',
  101: 'Стіни сирі',
  476: 'Після повені',
  477: 'Після пожежі',
  105: 'Тільки документи',
  783: 'Від будівельників (з обробкою)',
  800: 'Після будівельників',
};

const PROJECT_TYPE_MAP: Record<number, string> = {
  79: 'Новобуд',
  76: 'Моноліт',
  4: 'Висотний будинок',
  82: 'Сталінка',
  84: 'Чеська',
  644: 'Царський (дореволюційний)',
  6: 'Московський',
  3: 'Болгарська',
  11: 'Харківський',
  60: 'Югославський',
  83: 'Хрущовка',
  5: 'Малосімейка',
  85: 'Гостинка',
  7: 'Прихована вітальня',
  9: 'Спецпроект',
  10: 'Старий фонд',
  8: 'Сотовий',
  86: 'Бельгійська',
  723: 'Польська',
  724: 'Польський люкс',
  725: 'Польський напівлюкс',
  726: 'Австрійська',
  727: 'Австрійський люкс',
  764: 'Київка',
  630: '134',
  631: '464',
  632: '96',
  633: 'АПВС',
  634: 'АППС',
  635: 'АППС люкс',
  637: 'БПС',
  638: 'КП',
  639: 'КТ',
  640: 'КТУ',
  641: 'Т1',
  642: 'Т2',
  643: 'Т4',
};

const REALTOR_UA_STATUS_MAP: Record<string, string> = {
  'Євроремонт': 'Євроремонт',
  'Від будівельників (з обробкою)': 'Після будівельників',
  'Від будівельників вільне планування': 'Після будівельників',
  'Дизайнерський ремонт': 'Євроремонт',
  'Задовільний стан': 'Житлове чисте',
  'Незавершений ремонт': 'Потрібен косметичний ремонт',
  'Перша здача': 'Житлове чисте',
  'Потрібен капітальний ремонт': 'Потрібен капітальний ремонт',
  'Потрібен косметичний ремонт': 'Потрібен косметичний ремонт',
  'Хороший стан': 'Житлове чисте',
  'Чудовий стан': 'Євроремонт',
  'З ремонтом': 'Хороший стан',
  'Без ремонту': 'Потрібен косметичний ремонт',
  'Частковий ремонт': 'Хороший стан',
};

const REALTOR_UA_BORDER_MAP: Record<string, string> = {
  'Бетонно монолітний': 'Моноліт',
  'Будинок': 'Будинок',
  'Газоблок': 'Газобетон',
  'Дача': 'Дача',
  'Дореволюційний': 'Старий фонд',
  'Дуплекс': 'Дуплекс',
  'Котедж': 'Котедж',
  'Сталінка': 'Сталінка',
  'Стара панель': 'Хрущовка',
  'Стара цегла': 'Сталінка',
  'Таунхаус': 'Таунхаус',
  'Типова панель': 'Чеська',
  'Українська панель': 'Харківський',
  'Українська цегла': 'Спецпроект',
  'Частина будинку': 'Частина будинку',
  'Монолітно-каркасна': 'Моноліт',
  'Цегляна': 'Спецпроект',
  'Утеплена панель': 'Чеська',
  'Панельна': 'Хрущовка',
  'Блочна': 'Спецпроект',
};

const OLX_REPAIR_MAP: Record<string, string> = {
  'Євроремонт': 'Євроремонт',
  'Після будівельників': 'Після будівельників',
  'Авторський проект': 'Євроремонт',
  'Житловий стан': 'Житлове чисте',
  'Косметичний ремонт': 'Хороший стан',
  'Під чистову обробку': 'Після будівельників',
  'Аварійний стан': 'Потрібен капітальний ремонт',
};

const OLX_PROPERTY_TYPE_MAP: Record<string, string> = {
  'Житловий фонд від 2021 р.': 'Новобуд',
  'Житловий фонд 2011-2020-і': 'Новобуд',
  'Житловий фонд від 2011 р.': 'Новобуд',
  'Житловий фонд 2001-2010-і': 'Спецпроект',
  'Чешка': 'Чеська',
  'Хрущовка': 'Хрущовка',
  'Житловий фонд 80-90-і': 'Спецпроект',
  'Житловий фонд 91-2000-і': 'Спецпроект',
  'Сталінка': 'Сталінка',
  'Гостинка': 'Гостинка',
  'Совмін': 'Спецпроект',
  'Гуртожиток': 'Гостинка',
  'Царський будинок': 'Старий фонд',
  'Будинок до 1917 року': 'Старий фонд',
};

const DOMRIA_CONDITION_MAP: Record<number, string> = {
  507: 'Після будівельників',
  508: 'Потрібен капітальний ремонт',
  509: 'Потрібен косметичний ремонт',
  510: 'Євроремонт',
  511: 'Житлове чисте',
  512: 'Хороший стан',
  513: 'Після будівельників',
  514: 'Після будівельників',
  515: 'Євроремонт',
};

const DOMRIA_WALL_TYPE_MAP: Record<string, string> = {
  'кирпич': 'Спецпроект',
  'панель': 'Хрущовка',
  'газоблок': 'Газобетон',
  'монолитно-каркасный': 'Моноліт',
  'монолит': 'Моноліт',
  'газобетон': 'Газобетон',
  'керамический блок': 'Спецпроект',
  'монолитно-кирпичный': 'Моноліт',
  'пеноблок': 'Газобетон',
  'железобетон': 'Моноліт',
  'ракушечник (ракушняк)': 'Спецпроект',
  'керамзитобетон': 'Спецпроект',
  'монолитно-блочный': 'Моноліт',
  'силикатный кирпич': 'Спецпроект',
  'монолитный железобетон': 'Моноліт',
  'красный кирпич': 'Спецпроект',
  'блочно-кирпичный': 'Спецпроект',
  'керамический кирпич': 'Спецпроект',
  'цегла': 'Спецпроект',
};

function mapConditionType(conditionTypeId: unknown): string | undefined {
  const id = extractInteger(conditionTypeId);
  if (!id) return undefined;
  return CONDITION_TYPE_MAP[id];
}

function mapProjectType(projectId: unknown): string | undefined {
  const id = extractInteger(projectId);
  if (!id) return undefined;
  return PROJECT_TYPE_MAP[id];
}

function mapRealtorUaCondition(status: string | undefined): string | undefined {
  if (!status) return undefined;
  return REALTOR_UA_STATUS_MAP[status];
}

function mapRealtorUaHouseType(border: string | undefined): string | undefined {
  if (!border) return undefined;
  return REALTOR_UA_BORDER_MAP[border];
}

function getOlxParamValue(params: unknown, key: string): string | undefined {
  if (!Array.isArray(params)) return undefined;
  const param = params.find((p: any) => p?.key === key);
  return param?.value as string | undefined;
}

function mapOlxCondition(params: unknown): string | undefined {
  const repair = getOlxParamValue(params, 'repair');
  if (repair) {
    return OLX_REPAIR_MAP[repair];
  }
  const isRepaired = getOlxParamValue(params, 'is_repaired');
  if (isRepaired) {
    if (isRepaired === 'Так' || isRepaired === 'yes') return 'Євроремонт';
    if (isRepaired === 'Ні' || isRepaired === 'no') return 'Без ремонту';
  }
  return undefined;
}

function mapOlxHouseType(params: unknown): string | undefined {
  const propertyType = getOlxParamValue(params, 'property_type_appartments_sale');
  if (!propertyType) return undefined;
  return OLX_PROPERTY_TYPE_MAP[propertyType];
}

function mapDomriaCondition(characteristicsValues: unknown): string | undefined {
  if (!characteristicsValues || typeof characteristicsValues !== 'object') return undefined;
  const value = (characteristicsValues as Record<string, unknown>)['516'];
  const id = extractInteger(value);
  if (!id) return undefined;
  return DOMRIA_CONDITION_MAP[id];
}

function mapDomriaHouseType(wallType: string | undefined): string | undefined {
  if (!wallType) return undefined;
  return DOMRIA_WALL_TYPE_MAP[wallType.toLowerCase()];
}

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '2000', 10);
  const startOffset = parseInt(args[1] || '0', 10);

  logger.log('='.repeat(60));
  logger.log('Sync Aggregator FAST (with geo caching)');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Start offset: ${startOffset}`);
  logger.log('='.repeat(60));

  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);
  const geoLookupService = app.get(GeoLookupService);

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

  // Get total count (ALL records)
  const countResult = await dataSource.query(`
    SELECT COUNT(*) as count FROM aggregator_import
    WHERE lat != '' AND lng != ''
  `);
  const totalCount = parseInt(countResult[0].count, 10);
  logger.log(`Total records with coordinates: ${totalCount}`);

  let totalSynced = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let geoResolvedByCoords = 0;
  let streetResolvedByCoords = 0;
  let batchNumber = 0;
  let currentOffset = startOffset;

  const startTime = Date.now();

  try {
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
      logger.log(`\nBatch ${batchNumber}: Processing ${properties.length} records (offset: ${currentOffset})`);

      // Pre-fetch topzones and complexes
      const topzoneIds = [...new Set(properties.map(p => extractInteger(p.topzone_id)).filter(Boolean))];
      const complexIds = [...new Set(properties.map(p => extractInteger(p.complex_id)).filter(Boolean))];

      const [existingTopzones, existingComplexes] = await Promise.all([
        topzoneIds.length > 0 ? topzoneRepository.find({ where: topzoneIds.map(id => ({ id })), select: ['id'] }) : [],
        complexIds.length > 0 ? complexRepository.find({ where: complexIds.map(id => ({ id })), select: ['id'] }) : [],
      ]);

      const validTopzoneIds = new Set(existingTopzones.map(t => t.id));
      const validComplexIds = new Set(existingComplexes.map(c => c.id));

      let synced = 0;
      let skipped = 0;
      let errors = 0;

      const CONCURRENCY = 100;
      const listingsToUpsert: Partial<UnifiedListing>[] = [];

      const processItem = async (item: ImportedProperty): Promise<Partial<UnifiedListing> | null> => {
        try {
          const attributes = parseJsonSafe(item.attributes);
          const primaryData = parseJsonSafe(item.primary_data);
          const lat = extractNumber(item.lat);
          const lng = extractNumber(item.lng);
          const platform = item.realty_platform;

          if (!lat || !lng) return null;

          let validGeoId: number | undefined = undefined;
          let validStreetId: number | undefined = undefined;
          let validTopzoneId: number | undefined = undefined;
          let validComplexId: number | undefined = undefined;

          const topzoneIdNum = extractInteger(item.topzone_id);
          const complexIdNum = extractInteger(item.complex_id);

          validTopzoneId = topzoneIdNum && validTopzoneIds.has(topzoneIdNum) ? topzoneIdNum : undefined;
          validComplexId = complexIdNum && validComplexIds.has(complexIdNum) ? complexIdNum : undefined;

          // FAST geo resolution - uses caching!
          const resolved = await geoLookupService.resolveGeoForListing(lng, lat);
          if (resolved.geoId) {
            validGeoId = resolved.geoId;
            geoResolvedByCoords++;
          }
          if (resolved.streetId) {
            validStreetId = resolved.streetId;
            streetResolvedByCoords++;
          }

          if (!validGeoId) return null;

          const sourceId = extractInteger(item.id);
          if (!sourceId) return null;

          const description = parseJsonSafe(item.description);
          const mainParams = primaryData?.['main_params'] as Record<string, unknown> | undefined;

          // Resolve condition
          const condition = (() => {
            if (platform === 'realtorUa' && mainParams?.['status']) {
              const result = mapRealtorUaCondition(mainParams['status'] as string);
              if (result) return result;
            }
            if (platform === 'olx' && primaryData?.['params']) {
              const olxCondition = mapOlxCondition(primaryData['params']);
              if (olxCondition) return olxCondition;
            }
            if (platform === 'domRia' && primaryData?.['characteristics_values']) {
              const domriaCondition = mapDomriaCondition(primaryData['characteristics_values']);
              if (domriaCondition) return domriaCondition;
            }
            return mapConditionType(attributes?.condition_type) || (attributes?.condition as string);
          })();

          // Resolve houseType
          const houseType = (() => {
            if (platform === 'realtorUa' && mainParams?.['border']) {
              const result = mapRealtorUaHouseType(mainParams['border'] as string);
              if (result) return result;
            }
            if (platform === 'olx' && primaryData?.['params']) {
              const olxHouseType = mapOlxHouseType(primaryData['params']);
              if (olxHouseType) return olxHouseType;
            }
            if (platform === 'domRia' && primaryData?.['wall_type']) {
              const domriaHouseType = mapDomriaHouseType(primaryData['wall_type'] as string);
              if (domriaHouseType) return domriaHouseType;
            }
            return mapProjectType(attributes?.project) || (attributes?.houseType as string);
          })();

          return {
            sourceType: SourceType.AGGREGATOR,
            sourceId,
            dealType: mapDealType(item.deal_type),
            realtyType: mapRealtyType(item.realty_type),
            geoId: validGeoId,
            streetId: validStreetId,
            topzoneId: validTopzoneId,
            complexId: validComplexId,
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
              if (price && price > 0 && area && area > 0) {
                return Math.round(price / area);
              }
              return undefined;
            })(),
            totalArea: extractNumber(attributes?.square_total ?? attributes?.totalArea),
            livingArea: extractNumber(attributes?.square_living ?? attributes?.livingArea),
            kitchenArea: extractNumber(attributes?.square_kitchen ?? attributes?.kitchenArea),
            rooms: extractInteger(attributes?.rooms_count ?? attributes?.rooms),
            floor: extractInteger(attributes?.floor),
            totalFloors: extractInteger(attributes?.floors_count ?? attributes?.totalFloors),
            condition,
            houseType,
            attributes: attributes,
            description: description as unknown as MultiLanguageDto,
            externalUrl: item.external_url,
            isActive: item.is_active === 't' || item.is_active === 'true' || item.is_active === '1',
            publishedAt: item.created_at && item.created_at !== '' ? new Date(item.created_at) : undefined,
            deletedAt: item.deleted_at && item.deleted_at !== '' ? new Date(item.deleted_at) : undefined,
            syncedAt: new Date(),
          };
        } catch (error) {
          if (errors < 5) {
            logger.error(`Failed to process property ${item.id || 'unknown'}: ${(error as Error).message}`);
          }
          errors++;
          return null;
        }
      };

      // Process in parallel chunks
      for (let i = 0; i < properties.length; i += CONCURRENCY) {
        const chunk = properties.slice(i, i + CONCURRENCY);
        const results = await Promise.all(chunk.map(processItem));

        for (const result of results) {
          if (result !== null) {
            listingsToUpsert.push(result);
          } else {
            skipped++;
          }
        }
      }

      // Bulk upsert
      if (listingsToUpsert.length > 0) {
        try {
          await listingRepository.upsert(listingsToUpsert as any, {
            conflictPaths: ['sourceType', 'sourceId'],
            skipUpdateIfNoValuesChanged: true,
          });
          synced = listingsToUpsert.length;
        } catch (error) {
          logger.error(`Bulk upsert failed: ${(error as Error).message}`);
          errors += listingsToUpsert.length;
        }
      }

      totalSynced += synced;
      totalSkipped += skipped;
      totalErrors += errors;
      currentOffset += batchSize;

      const batchTime = ((Date.now() - batchStart) / 1000).toFixed(1);
      const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const progress = Math.round((currentOffset / totalCount) * 100);
      const recordsPerSec = Math.round(synced / parseFloat(batchTime));
      const eta = totalCount > currentOffset
        ? Math.round(((totalCount - currentOffset) / recordsPerSec / 60))
        : 0;

      logger.log(`Batch done in ${batchTime}s (${recordsPerSec} rec/s) | Progress: ${progress}% | ETA: ${eta} min`);
      logger.log(`Totals: synced=${totalSynced}, skipped=${totalSkipped}, errors=${totalErrors}`);
    }

    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.log('\n' + '='.repeat(60));
    logger.log('Processing completed');
    logger.log('='.repeat(60));
    logger.log(`Total time: ${totalTime} minutes`);
    logger.log(`Total synced: ${totalSynced}`);
    logger.log(`Total skipped: ${totalSkipped}`);
    logger.log(`Total errors: ${totalErrors}`);
    logger.log(`Geo resolved: ${geoResolvedByCoords}`);
    logger.log(`Streets resolved: ${streetResolvedByCoords}`);
    logger.log('='.repeat(60));
  } finally {
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Processing failed:', error);
  process.exit(1);
});
