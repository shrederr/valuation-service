import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { Vector2ExportDto, Vector2ExportAttributes } from '../dto';
import { PrimaryDataExtractor } from '../services/primary-data-extractor';

const DOMRIA_CDN = 'https://cdn.dom.ria.com/';

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
    if (listing.livingArea) result.square_living = listing.livingArea;
    if (listing.kitchenArea) result.square_kitchen = listing.kitchenArea;
    if (listing.landArea) result.square_land_total = Math.round((listing.landArea / 100) * 100) / 100;

    // Rooms & floors
    if (listing.rooms) result.rooms_count = listing.rooms;
    if (listing.floor) result.floor = listing.floor;
    if (listing.totalFloors) result.floors_count = listing.totalFloors;

    // Classification — reverse map to CRM IDs
    result.object_type = this.resolveObjectType(listing, attrs);
    result.condition_type = this.resolveConditionType(listing, attrs);
    result.project = this.resolveProject(listing, attrs);

    // Pass through remaining numeric attribute IDs
    const numericPassthrough = ['housing_material', 'apartment_type', 'ceiling_height'] as const;
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
    const raw = attrs.condition_type;
    if (raw !== undefined && raw !== null) {
      const num = Number(raw);
      if (!isNaN(num) && num >= 1 && num <= 30) return num;
    }
    // Reverse map from condition string
    if (listing.condition) {
      const mapped = REVERSE_CONDITION_MAP[listing.condition];
      if (mapped) return mapped;
    }
    return undefined;
  }

  /** Resolve project (house type): reverse map from houseType string → CRM ID */
  private resolveProject(listing: UnifiedListing, attrs: Record<string, unknown>): number | undefined {
    // If attrs has a small CRM-range project, use directly (vector2 IDs go up to 39)
    const raw = attrs.project;
    if (raw !== undefined && raw !== null) {
      const num = Number(raw);
      if (!isNaN(num) && num >= 1 && num <= 40) return num;
    }
    // Reverse map from houseType string
    if (listing.houseType) {
      const mapped = REVERSE_PROJECT_MAP[listing.houseType];
      if (mapped) return mapped;
    }
    return undefined;
  }

  /** Build photo URLs, handling platform-specific formats */
  private buildPhotos(listing: UnifiedListing, extractedPhotos: string[] | null): string[] | null {
    // domRia: photos is an object {id: {file: "dom/photo/..."}} — need to build full URLs
    if (listing.realtyPlatform === 'domRia') {
      const pd = (listing as any).primaryData || {};
      const photosObj = pd.photos;
      if (photosObj && typeof photosObj === 'object' && !Array.isArray(photosObj)) {
        const entries = Object.values(photosObj) as Array<{ file?: string; ordering?: number }>;
        return entries
          .filter(e => e.file && !e.file.includes('..'))
          .sort((a, b) => (a.ordering || 0) - (b.ordering || 0))
          .map(e => DOMRIA_CDN + e.file);
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
