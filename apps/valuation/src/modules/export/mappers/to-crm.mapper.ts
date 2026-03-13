import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { Vector2ExportDto, Vector2ExportAttributes } from '../dto';
import { PrimaryDataExtractor } from '../services/primary-data-extractor';

const DOMRIA_CDN = 'https://cdn.riastatic.com/photosnew/dom/photo/';

/** Reverse mapping: condition string → CRM condition_type ID */
const REVERSE_CONDITION_MAP: Record<string, number> = {
  'Потрібен капітальний ремонт': 1,
  'Потрібен поточний ремонт': 2,
  'Потрібен косметичний ремонт': 3,
  'Після капремонту': 4,
  'Євроремонт': 5,
  'Дизайнерський ремонт': 5,
  'Чудовий стан': 5,
  'Будинок, що будується': 6,
  'Будинок що будується': 6,
  'Після будівельників': 7,
  'Від будівельників вільне планування': 7,
  'Від будівельників (з обробкою)': 7,
  'Перша здача': 7,
  'Після пожежі': 8,
  'Після повені': 9,
  'Стіни сирі': 10,
  'Під знос': 11,
  'Недобудоване': 12,
  'Нуль цикл': 13,
  'Будматеріали': 14,
  'Тільки документи': 15,
  'Дах потрібний ремонт': 16,
  'Потріб. капрем. та дах': 17,
  'Потріб. тек. рем. та дах': 18,
  'Потріб. космет. рем. та дах': 19,
  'Житлове чисте': 20,
  'Задовільний стан': 20,
  'Хороший стан': 20,
  'Після косметики': 21,
  'Косметичний ремонт': 21,
  'Ремонт не потрібний': 22,
  'White Box': 24,
  'Аварійний': 1,
  'Без ремонту': 3,
};

/** Reverse mapping: house_type string → CRM project ID */
const REVERSE_PROJECT_MAP: Record<string, number> = {
  'Сталінка': 1,
  'Старий фонд': 2,
  'Висотний будинок': 3,
  'Хрущовка': 4,
  'Гостинка': 5,
  'Новобуд': 7,
  'Чеська': 8,
  'Моноліт': 9,
  'Спецпроект': 10,
  'Московський': 11,
  'Харківський': 12,
  'Болгарська': 14,
  'Малосімейка': 15,
  'Югославський': 16,
  'Бельгійська': 17,
  'Сотовий': 18,
  'Прихована вітальня': 19,
  'Царський (дореволюційний)': 33,
  'Польська': 34,
  'Польський люкс': 35,
  'Польський напівлюкс': 36,
  'Австрійська': 37,
  'Австрійський люкс': 38,
  'Київка': 39,
  // Housing material fallbacks (когда houseType = материал стен)
  'Газобетон': 10,
  // RealtorUA variants (description.details "Тип будинку - ...")
  'Хрущівка': 4,
  'Спец': 10,         // Спецпроект
  'Чеський проект': 8, // Чеська
  'Дореволюційний': 33,
  'новобудова': 7,     // Новобуд
};

/** Reverse mapping: layout string → CRM location_rooms ID */
const REVERSE_LOCATION_ROOMS_MAP: Record<string, number> = {
  'Роздільна': 1,
  'Роздільне': 1,
  'Раздельная': 1,
  'Суміжна': 2,
  'Суміжне': 2,
  'Смежная': 2,
  'Суміжно-роздільна': 3,
  'Суміжно-роздільне': 3,
  'Смежно-раздельная': 3,
  'Розпашонка': 4,
  'Напіврозпашонка': 5,
  'Трамвайчик': 6,
  'Гостинка': 7,
  'Малосімейка': 8,
  'Харківка': 9,
  'Московський': 10,
  'Болгарська': 11,
  'Чеська': 12,
  'Мінічешка': 13,
  'Югославський': 14,
  'Бельгійська': 15,
  '2 рівні': 16,
  'Дворівнева': 16,
  'Двухуровневая': 16,
  '3 рівні': 17,
  'Багаторівнева': 17,
  'Многоуровневая': 17,
  '4 рівні': 18,
  'Прохідна': 19,
  'Суміжно-паралельне': 20,
  'Перепланування': 21,
  'Трамвай ділений': 22,
  'Тупикова': 23,
  'Вільне планування': 24,
  'Свободная планировка': 24,
  'Студія': 24,
  'Студио': 24,
  'Євро двушка': 25,
  'Кухня-вітальня': 25,
  'Кухня-гостиная': 25,
  'Євро трійка': 26,
};

/** Reverse mapping: housing material string → CRM housing_material ID.
 *  Used by RealtorUA (border), DomRia (wall_type), and description parsing. */
const REVERSE_HOUSING_MATERIAL_MAP: Record<string, number> = {
  // RealtorUA border values
  'Цегляна': 2,            // Цегла
  'Стара цегла': 2,        // Цегла
  'Українська цегла': 2,   // Цегла
  'Панельна': 7,            // Панельний
  'Стара панель': 7,        // Панельний
  'Типова панель': 7,       // Панельний
  'Українська панель': 7,   // Панельний
  'Утеплена панель': 7,     // Панельний
  'Блочна': 8,              // Блоковий
  'Бетонно монолітний': 26, // Моноліт
  'Монолітно-каркасна': 26, // Моноліт
  'Газоблок': 28,           // Газобетон
  // DomRia wall_type values (Russian from API)
  'кирпич': 2,              // Цегла
  'красный кирпич': 2,
  'силикатный кирпич': 2,
  'керамический кирпич': 2,
  'цегла': 2,
  'панель': 7,              // Панельний
  'газоблок': 28,           // Газобетон
  'газобетон': 28,
  'пеноблок': 28,
  'монолит': 26,            // Моноліт
  'монолитно-каркасный': 26,
  'монолитно-кирпичный': 26,
  'монолитно-блочный': 26,
  'монолитный железобетон': 26,
  'железобетон': 26,
  'керамический блок': 8,   // Блоковий
  'блочно-кирпичный': 8,
  'керамзитобетон': 9,        // Керамзит-бетон (was 8/Блоковий — 9 is correct CRM key)
  'ракушечник (ракушняк)': 2,
  // RealEstateLvivUa details["Матеріал стін"] values (Ukrainian)
  'керамічний блок': 30,     // Керамічний блок
  'залізобетон': 14,         // Залізобетон
  'пінобетон (піноблок)': 27, // Пінобетон
  'дерево': 6,               // Дерево
  'газобетон (газоблок)': 28, // Газобетон
  'керамзитобетон, цегла': 9, // Керамзит-бетон (primary)
  'цегла, керамоблок': 2,    // Цегла (primary)
};

/** RealtorUA description.details condition → CRM condition_type ID.
 *  Values come from "Загальний стан квартири/будинку - ..." in description.details.
 *  Chain: details value → REALTOR_UA_STATUS_MAP → REVERSE_CONDITION_MAP → CRM ID */
const REALTOR_DETAILS_CONDITION_MAP: Record<string, number> = {
  'З ремонтом': 20,           // → Хороший стан → Житлове чисте (20)
  'Без ремонту': 3,           // → Потрібен косметичний ремонт (3)
  'Частковий ремонт': 20,     // → Хороший стан → Житлове чисте (20)
  'Хороший стан': 20,         // → Житлове чисте (20)
  'Євроремонт': 5,            // → Євроремонт (5)
  'Дизайнерський ремонт': 5,  // → Євроремонт (5)
  'Після ремонту': 4,         // → Після капремонту (4)
};

/** object_type defaults per realty type for CRM import */
const OBJECT_TYPE_MAP: Record<string, number> = {
  apartment: 1,    // вторинка (default)
  house: 6,        // будинок
  commercial: 3,   // комерція
  area: 5,         // земля
  garage: 3,       // комерція fallback
};

/** source_platform naming for CRM */
const PLATFORM_NAME_MAP: Record<string, string> = {
  olx: 'OLX',
  domRia: 'DomRia',
  realtorUa: 'RealtorUa',
  realEstateLvivUa: 'RealEstateLvivUa',
  mlsUkraine: 'MlsUkraine',
  unknown: 'Unknown',
};

@Injectable()
export class ToCrmMapper {
  private readonly logger = new Logger(ToCrmMapper.name);
  private geoCache = new Map<number, number | null>();
  private streetCache = new Map<number, number | null>();

  constructor(
    private readonly dataSource: DataSource,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
  ) {}

  async map(listing: UnifiedListing): Promise<Vector2ExportDto> {
    const extracted = this.primaryDataExtractor.extractForExport(listing);
    const attrs = listing.attributes || {};

    const photos = this.buildPhotos(listing, extracted.photos);
    const phone = extracted.phones?.[0] || null;

    const dto: Vector2ExportDto = {
      external_id: listing.sourceId,
      type_estate: this.mapTypeEstate(listing.realtyType),
      geo_id: listing.geoId ? await this.resolveGeoId(listing.geoId) : undefined,
      street_id: listing.streetId ? await this.resolveStreetId(listing.streetId) : undefined,
      source_platform: PLATFORM_NAME_MAP[listing.realtyPlatform || ''] || listing.realtyPlatform || 'Unknown',
      url: extracted.url || undefined,
      photos: photos?.length ? photos : undefined,
      attributes: this.buildAttributes(listing, attrs, extracted, phone),
    };

    return dto;
  }

  private buildAttributes(
    listing: UnifiedListing,
    attrs: Record<string, unknown>,
    extracted: { description: string | null; phones: string[] | null },
    phone: string | null,
  ): Vector2ExportAttributes {
    const result: Vector2ExportAttributes = {
      price: listing.price || 0,
      currency: listing.currency || 'USD',
    };

    // Address
    if (listing.houseNumber) {
      result.secr_addres_hous_numb = String(listing.houseNumber);
    }

    // Area
    if (listing.totalArea) result.square_total = listing.totalArea;
    if (listing.livingArea) {
      result.square_living = listing.livingArea;
    } else {
      // Fallback: try primaryData, then calculate from total - kitchen
      const pdLiving = this.extractLivingAreaFromPrimaryData(listing);
      if (pdLiving && pdLiving > 0) {
        result.square_living = pdLiving;
      } else if (listing.totalArea && listing.totalArea > 0 && listing.kitchenArea && listing.kitchenArea > 0) {
        // living = total - kitchen (only when both values are available)
        const calculated = Math.round(listing.totalArea - listing.kitchenArea);
        if (calculated > 0 && calculated < listing.totalArea) {
          result.square_living = calculated;
        }
      }
      // If no living area could be determined → square_living stays undefined
      // Export service will skip objects without square_living
    }
    if (listing.kitchenArea) result.square_kitchen = listing.kitchenArea;
    // Land area: prefer attributes.square_land_total (already in sotki from aggregator)
    // Fallback to listing.landArea (m²) → convert to sotki (/100)
    const landSotki = this.extractNumber(attrs.square_land_total);
    if (landSotki && landSotki > 0) {
      result.square_land_total = landSotki;
    } else if (listing.landArea && listing.landArea > 0 && listing.landArea < 10000000) {
      // Sanity check: max ~100K sotki = 10M m² (anything bigger is corrupted data)
      result.square_land_total = Math.round((listing.landArea / 100) * 100) / 100;
    }

    // Rooms & floors
    if (listing.rooms) result.rooms_count = listing.rooms;
    if (listing.floor) result.floor = listing.floor;
    if (listing.totalFloors) result.floors_count = listing.totalFloors;

    // Classification — reverse map to CRM IDs
    result.object_type = this.resolveObjectType(listing, attrs);
    result.condition_type = this.resolveConditionType(listing, attrs);
    result.project = this.resolveProject(listing, attrs);
    result.location_rooms = this.resolveLocationRooms(listing, attrs);

    // Housing material — resolve from attrs or primaryData
    result.housing_material = this.resolveHousingMaterial(listing, attrs);

    // Pass through remaining numeric attribute IDs
    const numericPassthrough = ['apartment_type', 'ceiling_height'] as const;
    for (const key of numericPassthrough) {
      const val = attrs[key];
      if (val !== undefined && val !== null) {
        const num = Number(val);
        if (!isNaN(num)) {
          (result as any)[key] = num;
        }
      }
    }

    // Coordinates as strings
    if (listing.lat) result.map_x = String(listing.lat);
    if (listing.lng) result.map_y = String(listing.lng);

    // Descriptions (two languages)
    if (listing.description?.uk) {
      result.description_rekl_ua = listing.description.uk;
    }
    if (listing.description?.ru) {
      result.description_rekl = listing.description.ru;
    }

    // Phone (single, first one)
    if (phone) {
      result.secr_owner_phone = this.formatPhone(phone);
    }

    // Country
    result.country1 = 'UA';

    // Deal type specific
    if (listing.dealType === 'rent') {
      result.currency_rent = listing.currency || 'USD';
    }

    return result;
  }

  /** Resolve object_type: try CRM ID from attrs, fallback to realty type default */
  private resolveObjectType(listing: UnifiedListing, attrs: Record<string, unknown>): number {
    // If attrs has a small CRM-range object_type (1-6), use it directly
    const raw = attrs.object_type;
    if (raw !== undefined && raw !== null) {
      const num = Number(raw);
      if (!isNaN(num) && num >= 1 && num <= 6) return num;
    }
    // For apartments: detect new build vs secondary
    if (listing.realtyType === 'apartment') {
      // 1) house_type = 'Новобуд' — most reliable
      if (listing.houseType === 'Новобуд') return 2;
      // 2) OLX: primaryData.params
      const pd = (listing as any).primaryData;
      if (pd?.params && Array.isArray(pd.params)) {
        const objType = pd.params.find((p: any) => p?.key === 'apartments_object_type');
        if (objType?.normalizedValue === 'primary_market') return 2;
      }
    }
    return OBJECT_TYPE_MAP[listing.realtyType] || 1;
  }

  /** Resolve condition_type: reverse map from condition string → CRM ID */
  private resolveConditionType(listing: UnifiedListing, attrs: Record<string, unknown>): number | undefined {
    // If attrs has a small CRM-range condition_type, use directly (vector2 IDs: 1-24)
    // Skip for realtorUa/domRia — their attrs.condition_type are platform-internal IDs, not CRM IDs
    if (listing.realtyPlatform !== 'realtorUa' && listing.realtyPlatform !== 'domRia') {
      const raw = attrs.condition_type;
      if (raw !== undefined && raw !== null) {
        const num = Number(raw);
        if (!isNaN(num) && num >= 1 && num <= 30) return num;
      }
    }
    // Reverse map from condition string
    if (listing.condition) {
      const mapped = REVERSE_CONDITION_MAP[listing.condition];
      if (mapped) return mapped;
    }
    // Fallback for realtorUa: extract from description.details
    // Pattern: "Загальний стан квартири/будинку - З ремонтом"
    // ~34K objects have condition here but not in main_params.status
    if (listing.realtyPlatform === 'realtorUa') {
      const pd = (listing as any).primaryData;
      const details = pd?.description?.details;
      if (typeof details === 'string') {
        const match = details.match(/Загальний стан (?:квартири|будинку|приміщення)\s*[-–—]\s*([^.]+)/);
        if (match?.[1]) {
          const statusFromDetails = match[1].trim();
          const mapped = REALTOR_DETAILS_CONDITION_MAP[statusFromDetails];
          if (mapped) return mapped;
        }
      }
    }
    return undefined;
  }

  /** Resolve project (house type): reverse map from houseType string → CRM ID */
  private resolveProject(listing: UnifiedListing, attrs: Record<string, unknown>): number | undefined {
    // If attrs has a small CRM-range project, use directly (vector2 IDs go up to 39)
    // Skip for realtorUa/domRia — their attrs.project are platform-internal border IDs, not CRM IDs
    if (listing.realtyPlatform !== 'realtorUa' && listing.realtyPlatform !== 'domRia') {
      const raw = attrs.project;
      if (raw !== undefined && raw !== null) {
        const num = Number(raw);
        if (!isNaN(num) && num >= 1 && num <= 40) return num;
      }
    }
    // Extract from primaryData FIRST (more precise than entity houseType which may come from
    // incorrectly mapped border/wall_type values)
    const pd = (listing as any).primaryData;
    if (pd) {
      // OLX: apartments_object_type = "Новобудова" → project 7
      if (Array.isArray(pd.params)) {
        const objType = pd.params.find((p: any) => p?.key === 'apartments_object_type');
        if (objType?.normalizedValue === 'primary_market' || objType?.value === 'Новобудова') {
          return 7; // Новобуд
        }
        // OLX: property_type_appartments_sale
        const houseType = pd.params.find((p: any) => p?.key === 'property_type_appartments_sale');
        if (houseType?.value) {
          const mapped = REVERSE_PROJECT_MAP[houseType.value];
          if (mapped) return mapped;
        }
      }
      // realtorUa: description.details contains "Тип будинку - Хрущівка"
      const details = pd.description?.details;
      if (typeof details === 'string') {
        const houseTypeMatch = details.match(/Тип будинку\s*[-–—]\s*([^,.]+)/);
        if (houseTypeMatch?.[1]) {
          const houseTypeName = houseTypeMatch[1].trim();
          const mapped = REVERSE_PROJECT_MAP[houseTypeName];
          if (mapped) return mapped;
        }
      }
      // realtorUa: main_params.border — only actual house types (not materials)
      if (pd.main_params?.border) {
        const mapped = REVERSE_PROJECT_MAP[pd.main_params.border];
        if (mapped) return mapped;
      }
    }
    // Fallback: reverse map from entity houseType string
    // Skip DomRia (was incorrectly set from wall_type) and RealtorUA (may be stale material-based value)
    if (listing.houseType && listing.realtyPlatform !== 'domRia' && listing.realtyPlatform !== 'realtorUa') {
      const mapped = REVERSE_PROJECT_MAP[listing.houseType];
      if (mapped) return mapped;
    }
    return undefined;
  }

  // Valid CRM housing_material key values (from ob_attribute_items)
  private static readonly VALID_CRM_HOUSING_MATERIAL_KEYS = new Set([
    1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 14, 21, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
  ]);

  /** Resolve housing_material: attrs passthrough (only valid CRM keys) or extract from primaryData */
  private resolveHousingMaterial(listing: UnifiedListing, attrs: Record<string, unknown>): number | undefined {
    // If attrs has a valid CRM housing_material key, use directly
    // NOTE: only vector_crm listings have correct CRM keys; aggregator platforms
    // (realEstateLvivUa, etc.) store their own IDs which may not be valid CRM keys
    const raw = attrs.housing_material;
    if (raw !== undefined && raw !== null) {
      const num = Number(raw);
      if (!isNaN(num) && ToCrmMapper.VALID_CRM_HOUSING_MATERIAL_KEYS.has(num)) return num;
    }
    // Extract from primaryData
    const pd = (listing as any).primaryData;
    if (pd) {
      // domRia: wall_type → housing material (wall_type is building MATERIAL, not project type)
      if (listing.realtyPlatform === 'domRia' && pd.wall_type) {
        const mapped = REVERSE_HOUSING_MATERIAL_MAP[pd.wall_type];
        if (mapped) return mapped;
        // Also try wall_type_uk
        if (pd.wall_type_uk) {
          const mappedUk = REVERSE_HOUSING_MATERIAL_MAP[pd.wall_type_uk];
          if (mappedUk) return mappedUk;
        }
      }
      // realtorUa: main_params.border → housing material
      if (pd.main_params?.border) {
        const mapped = REVERSE_HOUSING_MATERIAL_MAP[pd.main_params.border];
        if (mapped) return mapped;
      }
      // realtorUa: description.details contains "Технологія будівництва - Блочна"
      const details = pd.description?.details;
      if (typeof details === 'string') {
        const techMatch = details.match(/Технологія будівництва\s*[-–—]\s*([^,.]+)/);
        if (techMatch?.[1]) {
          const materialName = techMatch[1].trim();
          const mapped = REVERSE_HOUSING_MATERIAL_MAP[materialName];
          if (mapped) return mapped;
        }
      }
      // realEstateLvivUa: details object has "Матеріал стін" key
      if (pd.details && typeof pd.details === 'object' && !Array.isArray(pd.details)) {
        const materialStr = pd.details['Матеріал стін'];
        if (typeof materialStr === 'string') {
          const mapped = REVERSE_HOUSING_MATERIAL_MAP[materialStr.toLowerCase()];
          if (mapped) return mapped;
        }
      }
    }
    return undefined;
  }

  /** Extract living area from primaryData (OLX, RealtorUA, DomRia, MLS) */
  private extractLivingAreaFromPrimaryData(listing: UnifiedListing): number | undefined {
    const pd = (listing as any).primaryData;
    if (!pd) return undefined;

    // OLX: params key="living_area"
    if (Array.isArray(pd.params)) {
      const la = pd.params.find((p: any) => p?.key === 'living_area');
      if (la) {
        const val = parseFloat(la.normalizedValue || la.value);
        if (!isNaN(val) && val > 0) return val;
      }
    }

    // RealtorUA: main_params.living_square or total fields
    if (pd.main_params?.living_square) {
      const val = parseFloat(pd.main_params.living_square);
      if (!isNaN(val) && val > 0) return val;
    }

    // DomRia: characteristics living_area_total
    if (pd.characteristics?.living_area_total) {
      const val = parseFloat(pd.characteristics.living_area_total);
      if (!isNaN(val) && val > 0) return val;
    }
    if (Array.isArray(pd.characteristics_values)) {
      const la = pd.characteristics_values.find((c: any) =>
        c?.characteristic_id === 209 || c?.name === 'Житлова площа',
      );
      if (la) {
        const val = parseFloat(la.value);
        if (!isNaN(val) && val > 0) return val;
      }
    }

    // MLS: params key with living area
    if (pd.zagalna_ploscha_zhytlova || pd.living_area) {
      const val = parseFloat(pd.zagalna_ploscha_zhytlova || pd.living_area);
      if (!isNaN(val) && val > 0) return val;
    }

    return undefined;
  }

  /** Resolve location_rooms (планировка): reverse map from layout string → CRM ID */
  private resolveLocationRooms(listing: UnifiedListing, attrs: Record<string, unknown>): number | undefined {
    // If attrs has a CRM-range location_rooms ID, use directly
    const raw = attrs.location_rooms;
    if (raw !== undefined && raw !== null) {
      const num = Number(raw);
      if (!isNaN(num) && num >= 1 && num <= 26) return num;
    }
    // From entity column planningType
    if (listing.planningType) {
      const mapped = REVERSE_LOCATION_ROOMS_MAP[listing.planningType];
      if (mapped) return mapped;
    }
    // Extract from primaryData
    const pd = (listing as any).primaryData;
    if (pd) {
      // OLX: params key="layout"
      if (Array.isArray(pd.params)) {
        const layoutParam = pd.params.find((p: any) => p?.key === 'layout');
        if (layoutParam?.value) {
          const mapped = REVERSE_LOCATION_ROOMS_MAP[layoutParam.value];
          if (mapped) return mapped;
        }
      }
      // realtorUa: main_params.planirovka
      if (pd.main_params?.planirovka) {
        const mapped = REVERSE_LOCATION_ROOMS_MAP[pd.main_params.planirovka];
        if (mapped) return mapped;
      }
      // mlsUkraine: params.osoblyvosti_planuvannja
      if (pd.params?.osoblyvosti_planuvannja && typeof pd.params === 'object' && !Array.isArray(pd.params)) {
        const mapped = REVERSE_LOCATION_ROOMS_MAP[pd.params.osoblyvosti_planuvannja];
        if (mapped) return mapped;
      }
    }
    return undefined;
  }

  /** Build photo URLs, handling platform-specific formats */
  private extractNumber(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    const num = typeof value === 'number' ? value : parseFloat(String(value));
    return isNaN(num) ? null : num;
  }

  private buildPhotos(listing: UnifiedListing, extractedPhotos: string[] | null): string[] | null {
    // domRia: photos is an object {photoId: {file: "dom/photo/...", ordering: N}}
    // CDN format: https://cdn.riastatic.com/photosnew/dom/photo/{slug}__{photoId}f.jpg
    // slug comes from beautiful_url: "realty-prodaja-kvartira-...-{id}.html" → "prodaja-kvartira-..."
    if (listing.realtyPlatform === 'domRia') {
      const pd = (listing as any).primaryData || {};
      const photosObj = pd.photos;
      if (photosObj && typeof photosObj === 'object' && !Array.isArray(photosObj)) {
        // Extract slug from beautiful_url
        const beautifulUrl = pd.beautiful_url as string | undefined;
        let slug = '';
        if (beautifulUrl) {
          // "realty-prodaja-kvartira-vinnitsa-...-33753247.html" → "prodaja-kvartira-vinnitsa-..."
          slug = beautifulUrl
            .replace(/^realty-/, '')
            .replace(/-\d+\.html$/, '');
        }

        const photoIds = Object.keys(photosObj);
        const entries = photoIds
          .map(id => ({ id, ...(photosObj[id] as { file?: string; ordering?: number }) }))
          .filter(e => e.file && !e.file.includes('..'))
          .sort((a, b) => (a.ordering || 0) - (b.ordering || 0));

        if (slug) {
          // Slug-based CDN URL (reliable)
          return entries.map(e => `${DOMRIA_CDN}${slug}__${e.id}f.jpg`);
        }
        // Fallback: old path-based URL (may 415)
        return entries.map(e => `https://cdn.riastatic.com/photos/${e.file}`);
      }
    }

    // mlsUkraine: ad_img field
    if (listing.realtyPlatform === 'mlsUkraine') {
      const pd = (listing as any).primaryData || {};
      if (Array.isArray(pd.ad_img) && pd.ad_img.length > 0) {
        return pd.ad_img.map(String);
      }
    }

    return extractedPhotos;
  }

  /** Format phone to +380 XX XXX XXXX */
  private formatPhone(phone: string): string {
    const digits = phone.replace(/\D/g, '');
    // Normalize to 380XXXXXXXXX
    let normalized: string;
    if (digits.startsWith('380') && digits.length === 12) {
      normalized = digits;
    } else if (digits.startsWith('0') && digits.length === 10) {
      normalized = '38' + digits;
    } else {
      normalized = digits;
    }
    if (normalized.length === 12 && normalized.startsWith('380')) {
      return `+${normalized.slice(0, 3)} ${normalized.slice(3, 5)} ${normalized.slice(5, 8)} ${normalized.slice(8)}`;
    }
    return phone; // return as-is if can't normalize
  }

  private mapTypeEstate(realtyType: string): number {
    const map: Record<string, number> = {
      apartment: 1,
      house: 2,
      commercial: 3,
      area: 4,
      garage: 5,
      room: 6,
    };
    return map[realtyType] || 1;
  }

  /** Resolve local geoId -> vector2 source_id via source_id_mappings */
  private async resolveGeoId(localId: number): Promise<number | undefined> {
    if (this.geoCache.has(localId)) {
      return this.geoCache.get(localId) ?? undefined;
    }

    try {
      const result = await this.dataSource.query(
        `SELECT source_id FROM source_id_mappings WHERE local_id = $1 AND entity_type = 'geo' AND source = 'vector2_crm' LIMIT 1`,
        [localId],
      );
      const sourceId = result.length > 0 ? parseInt(result[0].source_id, 10) : null;
      this.geoCache.set(localId, sourceId);
      return sourceId ?? undefined;
    } catch {
      return undefined;
    }
  }

  /** Resolve local streetId -> vector2 source_id via source_id_mappings */
  private async resolveStreetId(localId: number): Promise<number | undefined> {
    if (this.streetCache.has(localId)) {
      return this.streetCache.get(localId) ?? undefined;
    }

    try {
      const result = await this.dataSource.query(
        `SELECT source_id FROM source_id_mappings WHERE local_id = $1 AND entity_type = 'street' AND source = 'vector2_crm' LIMIT 1`,
        [localId],
      );
      const sourceId = result.length > 0 ? parseInt(result[0].source_id, 10) : null;
      this.streetCache.set(localId, sourceId);
      return sourceId ?? undefined;
    } catch {
      return undefined;
    }
  }
}
