import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { SyncFromAggregatorDbModule } from './sync-from-aggregator-db.module';
import { UnifiedListing, Geo, Street, Topzone, ApartmentComplex } from '@libs/database';
import { SourceType, DealType, RealtyType, MultiLanguageDto } from '@libs/common';
import { GeoLookupService } from '../apps/valuation/src/modules/osm/geo-lookup.service';

const logger = new Logger('ProcessAggregatorDump');

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

/**
 * Маппинг condition_type ID → текстовое значение
 * Источник: vector-api.attribute_values WHERE attribute_key = 'condition_type'
 * + исправления для некорректных ID из aggregator (783, 800)
 */
const CONDITION_TYPE_MAP: Record<number, string> = {
  // Отличный
  81: 'Євроремонт',
  1144: 'Дизайнерський ремонт',
  1145: 'Чудовий стан',

  // Хороший
  69: 'Житлове чисте',
  100: 'Ремонт не потрібний',
  167: 'Хороший стан',

  // Новострой
  97: 'Від будівельників вільне планування',
  102: 'Будматеріали',
  103: 'Будинок що будується',
  169: 'Після будівельників',
  787: 'Від будівельників (з обробкою)',
  1146: 'Перша здача',

  // Средний
  99: 'Задовільний стан',
  96: 'Незавершений ремонт',

  // Требует ремонта
  95: 'Дах потрібний ремонт',
  106: 'Потріб. капрем. та дах',
  107: 'Потріб. космет. рем. та дах',
  108: 'Потріб. тек. рем. та дах',
  173: 'Потрібен капітальний ремонт',
  458: 'Потрібен косметичний ремонт',
  459: 'Потрібен поточний ремонт',

  // Плохой
  98: 'Аварійний',
  101: 'Стіни сирі',
  476: 'Після повені',
  477: 'Після пожежі',

  // Особый
  105: 'Тільки документи',

  // === ИСПРАВЛЕНИЯ ДЛЯ НЕКОРРЕКТНЫХ ID ИЗ AGGREGATOR ===
  // ID 783: используется domRia/realtorUa, не существует в vector-api
  // Маппим как аналог 787 "Від будівельників (з обробкою)"
  783: 'Від будівельників (з обробкою)',

  // ID 800: используется OLX, в vector-api это "appliances" (Домашній кінотеатр)
  // Маппим как аналог 169 "Після будівельників"
  800: 'Після будівельників',
};

/**
 * Маппинг project ID → текстовое значение (тип дома)
 * Источник: vector-api.attribute_values WHERE attribute_key = 'project'
 */
const PROJECT_TYPE_MAP: Record<number, string> = {
  // Современные
  79: 'Новобуд',
  76: 'Моноліт',
  4: 'Висотний будинок',

  // Советские - качественные
  82: 'Сталінка',
  84: 'Чеська',
  644: 'Царський (дореволюційний)',

  // Советские - стандартные
  6: 'Московський',
  3: 'Болгарська',
  11: 'Харківський',
  60: 'Югославський',

  // Советские - эконом
  83: 'Хрущовка',
  5: 'Малосімейка',
  85: 'Гостинка',
  7: 'Прихована вітальня',

  // Спецпроекты
  9: 'Спецпроект',
  10: 'Старий фонд',
  8: 'Сотовий',

  // Европейские
  86: 'Бельгійська',
  723: 'Польська',
  724: 'Польський люкс',
  725: 'Польський напівлюкс',
  726: 'Австрійська',
  727: 'Австрійський люкс',
  764: 'Київка',

  // Серии домов (специфические)
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

// ============================================================
// Маппінг для realtorUa з primary_data (сирі текстові значення)
// ============================================================

/**
 * Маппінг сирих значень status з realtorUa → нормалізоване condition
 * Джерело: primary_data.main_params.status
 */
const REALTOR_UA_STATUS_MAP: Record<string, string> = {
  // Від менеджера
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
  // Нові значення (не було в маппінгу менеджера)
  'З ремонтом': 'Хороший стан',
  'Без ремонту': 'Потрібен косметичний ремонт',
  'Частковий ремонт': 'Хороший стан',
};

/**
 * Маппінг сирих значень border з realtorUa → нормалізоване house_type
 * Джерело: primary_data.main_params.border
 */
const REALTOR_UA_BORDER_MAP: Record<string, string> = {
  // Від менеджера
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
  // Нові значення
  'Монолітно-каркасна': 'Моноліт',
  'Цегляна': 'Спецпроект',
  'Утеплена панель': 'Чеська',
  'Панельна': 'Хрущовка',
  'Блочна': 'Спецпроект',
};

// ============================================================
// Маппінг для OLX з primary_data.params
// ============================================================

/**
 * Маппінг сирих значень repair з OLX → нормалізоване condition
 * Джерело: primary_data.params де key='repair'
 */
const OLX_REPAIR_MAP: Record<string, string> = {
  'Євроремонт': 'Євроремонт',
  'Після будівельників': 'Після будівельників',
  'Авторський проект': 'Євроремонт',
  'Житловий стан': 'Житлове чисте',
  'Косметичний ремонт': 'Хороший стан',
  'Під чистову обробку': 'Після будівельників',
  'Аварійний стан': 'Потрібен капітальний ремонт',
};

/**
 * Маппінг сирих значень property_type_appartments_sale з OLX → нормалізоване house_type
 * Джерело: primary_data.params де key='property_type_appartments_sale'
 */
const OLX_PROPERTY_TYPE_MAP: Record<string, string> = {
  // Новобудови
  'Житловий фонд від 2021 р.': 'Новобуд',
  'Житловий фонд 2011-2020-і': 'Новобуд',
  'Житловий фонд від 2011 р.': 'Новобуд',
  'Житловий фонд 2001-2010-і': 'Спецпроект',
  // Радянський фонд
  'Чешка': 'Чеська',
  'Хрущовка': 'Хрущовка',
  'Житловий фонд 80-90-і': 'Спецпроект',
  'Житловий фонд 91-2000-і': 'Спецпроект',
  'Сталінка': 'Сталінка',
  'Гостинка': 'Гостинка',
  'Совмін': 'Спецпроект',
  'Гуртожиток': 'Гостинка',
  // Старий фонд
  'Царський будинок': 'Старий фонд',
  'Будинок до 1917 року': 'Старий фонд',
};

// ============================================================
// Маппінг для domRia з primary_data
// ============================================================

/**
 * Маппінг characteristics_values[516] з domRia → нормалізоване condition
 * 516 - це ID характеристики "стан ремонту" в domRia
 * Значення 507-515 відповідають різним станам
 */
const DOMRIA_CONDITION_MAP: Record<number, string> = {
  507: 'Після будівельників',    // від будівельників
  508: 'Потрібен капітальний ремонт', // потребує капремонту
  509: 'Потрібен косметичний ремонт', // потребує косметичного ремонту
  510: 'Євроремонт',             // євроремонт
  511: 'Житлове чисте',          // житловий стан
  512: 'Хороший стан',           // хороший стан
  513: 'Після будівельників',    // чорнова обробка
  514: 'Після будівельників',    // передчистова обробка
  515: 'Євроремонт',             // дизайнерський ремонт
};

/**
 * Маппінг wall_type з domRia → нормалізоване house_type
 * Джерело: primary_data.wall_type
 */
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

// Счетчики неизвестных значений для логирования
const unknownConditionTypes = new Map<number, number>();
const unknownProjectTypes = new Map<number, number>();
const unknownRealtorUaStatuses = new Map<string, number>();
const unknownRealtorUaBorders = new Map<string, number>();
const unknownOlxRepairs = new Map<string, number>();
const unknownOlxPropertyTypes = new Map<string, number>();
const unknownDomriaConditions = new Map<number, number>();
const unknownDomriaWallTypes = new Map<string, number>();

function mapConditionType(conditionTypeId: unknown): string | undefined {
  const id = extractInteger(conditionTypeId);
  if (!id) return undefined;
  const result = CONDITION_TYPE_MAP[id];
  if (!result) {
    unknownConditionTypes.set(id, (unknownConditionTypes.get(id) || 0) + 1);
  }
  return result;
}

function mapProjectType(projectId: unknown): string | undefined {
  const id = extractInteger(projectId);
  if (!id) return undefined;
  const result = PROJECT_TYPE_MAP[id];
  if (!result) {
    unknownProjectTypes.set(id, (unknownProjectTypes.get(id) || 0) + 1);
  }
  return result;
}

/**
 * Маппінг condition з primary_data для realtorUa
 */
function mapRealtorUaCondition(status: string | undefined): string | undefined {
  if (!status) return undefined;
  const result = REALTOR_UA_STATUS_MAP[status];
  if (!result && status) {
    unknownRealtorUaStatuses.set(status, (unknownRealtorUaStatuses.get(status) || 0) + 1);
  }
  return result;
}

/**
 * Маппінг house_type з primary_data для realtorUa
 */
function mapRealtorUaHouseType(border: string | undefined): string | undefined {
  if (!border) return undefined;
  const result = REALTOR_UA_BORDER_MAP[border];
  if (!result && border) {
    unknownRealtorUaBorders.set(border, (unknownRealtorUaBorders.get(border) || 0) + 1);
  }
  return result;
}

// ============================================================
// OLX mapping functions
// ============================================================

/**
 * Отримати значення з params масиву OLX
 */
function getOlxParamValue(params: unknown, key: string): string | undefined {
  if (!Array.isArray(params)) return undefined;
  const param = params.find((p: any) => p?.key === key);
  return param?.value as string | undefined;
}

/**
 * Маппінг condition з primary_data для OLX
 */
function mapOlxCondition(params: unknown): string | undefined {
  // Спочатку перевіряємо 'repair' (для житлової нерухомості)
  const repair = getOlxParamValue(params, 'repair');
  if (repair) {
    const result = OLX_REPAIR_MAP[repair];
    if (!result) {
      unknownOlxRepairs.set(repair, (unknownOlxRepairs.get(repair) || 0) + 1);
    }
    return result;
  }

  // Для комерційної нерухомості перевіряємо 'is_repaired'
  const isRepaired = getOlxParamValue(params, 'is_repaired');
  if (isRepaired) {
    if (isRepaired === 'Так' || isRepaired === 'yes') {
      return 'Євроремонт';
    }
    if (isRepaired === 'Ні' || isRepaired === 'no') {
      return 'Без ремонту';
    }
  }

  return undefined;
}

/**
 * Маппінг house_type з primary_data для OLX
 */
function mapOlxHouseType(params: unknown): string | undefined {
  const propertyType = getOlxParamValue(params, 'property_type_appartments_sale');
  if (!propertyType) return undefined;
  const result = OLX_PROPERTY_TYPE_MAP[propertyType];
  if (!result && propertyType) {
    unknownOlxPropertyTypes.set(propertyType, (unknownOlxPropertyTypes.get(propertyType) || 0) + 1);
  }
  return result;
}

// ============================================================
// domRia mapping functions
// ============================================================

/**
 * Маппінг condition з primary_data для domRia
 * Використовує characteristics_values[516]
 */
function mapDomriaCondition(characteristicsValues: unknown): string | undefined {
  if (!characteristicsValues || typeof characteristicsValues !== 'object') return undefined;
  const value = (characteristicsValues as Record<string, unknown>)['516'];
  const id = extractInteger(value);
  if (!id) return undefined;
  const result = DOMRIA_CONDITION_MAP[id];
  if (!result && id) {
    unknownDomriaConditions.set(id, (unknownDomriaConditions.get(id) || 0) + 1);
  }
  return result;
}

/**
 * Маппінг house_type з primary_data для domRia
 * Використовує wall_type
 */
function mapDomriaHouseType(wallType: string | undefined): string | undefined {
  if (!wallType) return undefined;
  const result = DOMRIA_WALL_TYPE_MAP[wallType.toLowerCase()];
  if (!result && wallType) {
    unknownDomriaWallTypes.set(wallType, (unknownDomriaWallTypes.get(wallType) || 0) + 1);
  }
  return result;
}

function logUnknownAttributeIds(): void {
  if (unknownConditionTypes.size > 0) {
    logger.warn('Unknown condition_type IDs found:');
    for (const [id, count] of unknownConditionTypes.entries()) {
      logger.warn(`  ID ${id}: ${count} occurrences`);
    }
  }
  if (unknownProjectTypes.size > 0) {
    logger.warn('Unknown project IDs found:');
    for (const [id, count] of unknownProjectTypes.entries()) {
      logger.warn(`  ID ${id}: ${count} occurrences`);
    }
  }
  if (unknownRealtorUaStatuses.size > 0) {
    logger.warn('Unknown realtorUa status values found:');
    for (const [status, count] of unknownRealtorUaStatuses.entries()) {
      logger.warn(`  "${status}": ${count} occurrences`);
    }
  }
  if (unknownRealtorUaBorders.size > 0) {
    logger.warn('Unknown realtorUa border values found:');
    for (const [border, count] of unknownRealtorUaBorders.entries()) {
      logger.warn(`  "${border}": ${count} occurrences`);
    }
  }
  if (unknownOlxRepairs.size > 0) {
    logger.warn('Unknown OLX repair values found:');
    for (const [repair, count] of unknownOlxRepairs.entries()) {
      logger.warn(`  "${repair}": ${count} occurrences`);
    }
  }
  if (unknownOlxPropertyTypes.size > 0) {
    logger.warn('Unknown OLX property_type values found:');
    for (const [type, count] of unknownOlxPropertyTypes.entries()) {
      logger.warn(`  "${type}": ${count} occurrences`);
    }
  }
  if (unknownDomriaConditions.size > 0) {
    logger.warn('Unknown domRia condition IDs found:');
    for (const [id, count] of unknownDomriaConditions.entries()) {
      logger.warn(`  ID ${id}: ${count} occurrences`);
    }
  }
  if (unknownDomriaWallTypes.size > 0) {
    logger.warn('Unknown domRia wall_type values found:');
    for (const [type, count] of unknownDomriaWallTypes.entries()) {
      logger.warn(`  "${type}": ${count} occurrences`);
    }
  }
}

// ============================================================
// Complex Matcher - finds ЖК by text in title/description
// ============================================================

interface ComplexInfo {
  id: number;
  nameRu: string;
  nameUk: string;
  nameNormalized: string;
  lat: number;
  lng: number;
  patterns: RegExp[];
}

class ComplexMatcher {
  private complexes: ComplexInfo[] = [];
  private loaded = false;
  // Cache for coordinate-based lookups (key: "lng,lat" rounded to 5 decimals)
  private coordCache = new Map<string, number | null>();
  private readonly CACHE_PRECISION = 5;

  private getCacheKey(lng: number, lat: number): string {
    return `${lng.toFixed(this.CACHE_PRECISION)},${lat.toFixed(this.CACHE_PRECISION)}`;
  }

  async load(dataSource: DataSource): Promise<void> {
    if (this.loaded) return;

    const rows = await dataSource.query(`
      SELECT id, name_ru, name_uk, name_normalized, lat, lng
      FROM apartment_complexes
      ORDER BY LENGTH(name_normalized) DESC
    `);

    this.complexes = rows.map((r: any) => ({
      id: r.id,
      nameRu: r.name_ru,
      nameUk: r.name_uk,
      nameNormalized: r.name_normalized,
      lat: parseFloat(r.lat),
      lng: parseFloat(r.lng),
      patterns: this.buildPatterns(r.name_ru, r.name_uk, r.name_normalized),
    }));

    this.loaded = true;
    logger.log(`ComplexMatcher: loaded ${this.complexes.length} complexes`);
  }

  private buildPatterns(nameRu: string, nameUk: string, normalized: string): RegExp[] {
    const patterns: RegExp[] = [];
    const names = [nameRu, nameUk].filter(Boolean);

    for (const name of names) {
      const cleaned = this.cleanName(name);
      // Skip very short or empty names
      if (cleaned.length < 4) continue;

      // Escape special regex chars
      const escaped = cleaned.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

      // For short names (< 8 chars), REQUIRE ЖК prefix to avoid false positives
      // Words like "море", "love", "smart" are too common
      if (cleaned.length < 8) {
        patterns.push(new RegExp(
          `(?:жк|жилой комплекс|житловий комплекс|кг|км)\\s*["«']?${escaped}["»']?`,
          'gi'
        ));
      } else {
        // Longer names: allow with optional prefix
        patterns.push(new RegExp(
          `(?:жк|жилой комплекс|житловий комплекс|кг|км)?\\s*["«']?${escaped}["»']?`,
          'gi'
        ));
        // Also just the name for long unique names (8+ chars)
        patterns.push(new RegExp(`\\b${escaped}\\b`, 'gi'));
      }
    }

    return patterns;
  }

  private cleanName(name: string): string {
    return name
      .replace(/^(жк|жилой комплекс|житловий комплекс|кг|км|котеджне|коттеджное|містечко|городок)\s*/gi, '')
      .replace(/["«»'']/g, '')
      .replace(/\s*\([^)]+\)\s*/g, ' ')
      .replace(/\s*буд\.?\s*\d+/gi, '')
      .trim();
  }

  /**
   * Find complex by matching text in title/description
   */
  findByText(title?: string, description?: string): ComplexInfo | null {
    const text = `${title || ''} ${this.extractDescriptionText(description)}`.toLowerCase();
    if (text.length < 10) return null;

    let bestMatch: ComplexInfo | null = null;
    let bestScore = 0;

    for (const complex of this.complexes) {
      for (const pattern of complex.patterns) {
        const match = text.match(pattern);
        if (match) {
          const matchedText = match[0];
          const score = matchedText.length / complex.nameNormalized.length;

          if (score > bestScore && score >= 0.5) {
            bestScore = score;
            bestMatch = complex;
          }
        }
      }
    }

    return bestMatch;
  }

  private extractDescriptionText(description?: string): string {
    if (!description) return '';

    // If JSON, extract text values
    try {
      const parsed = typeof description === 'string' ? JSON.parse(description) : description;
      if (typeof parsed === 'object') {
        // Extract uk/ru/en text values
        const texts: string[] = [];
        for (const key of ['uk', 'ru', 'en', 'ua']) {
          if (parsed[key] && typeof parsed[key] === 'string') {
            texts.push(parsed[key]);
          }
        }
        return texts.join(' ').substring(0, 1000);
      }
    } catch {
      // Not JSON, use as-is
    }

    return typeof description === 'string' ? description.substring(0, 1000) : '';
  }

  /**
   * Find complex by coordinates (point in polygon or nearest)
   */
  async findByCoordinates(dataSource: DataSource, lat: number, lng: number): Promise<number | null> {
    // Check cache first
    const cacheKey = this.getCacheKey(lng, lat);
    if (this.coordCache.has(cacheKey)) {
      return this.coordCache.get(cacheKey)!;
    }

    let result: number | null = null;

    try {
      // Try point-in-polygon first (only if polygon column exists and has data)
      const polygonResult = await dataSource.query(`
        SELECT id FROM apartment_complexes
        WHERE polygon IS NOT NULL
          AND ST_Contains(polygon, ST_SetSRID(ST_MakePoint($1, $2), 4326))
        LIMIT 1
      `, [lng, lat]);

      if (polygonResult.length > 0) {
        result = polygonResult[0].id;
      }
    } catch {
      // polygon column might not exist - skip point-in-polygon
    }

    if (!result) {
      // Fallback: nearest within 50m by centroid coordinates
      const nearest = await dataSource.query(`
        SELECT id FROM apartment_complexes
        WHERE ST_DWithin(
          ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
          ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
          50
        )
        ORDER BY ST_Distance(
          ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
          ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
        )
        LIMIT 1
      `, [lng, lat]);

      result = nearest.length > 0 ? nearest[0].id : null;
    }

    // Cache the result
    this.coordCache.set(cacheKey, result);
    return result;
  }
}

const complexMatcher = new ComplexMatcher();

async function main() {
  const args = process.argv.slice(2);
  const batchSize = parseInt(args[0] || '1000', 10);
  const startOffset = parseInt(args[1] || '0', 10);
  const maxBatches = parseInt(args[2] || '0', 10); // 0 = unlimited

  logger.log('='.repeat(60));
  logger.log('Process Aggregator Dump - Batch Processing');
  logger.log('='.repeat(60));
  logger.log(`Batch size: ${batchSize}`);
  logger.log(`Start offset: ${startOffset}`);
  logger.log(`Max batches: ${maxBatches || 'unlimited'}`);
  logger.log('='.repeat(60));

  // Create NestJS app context
  const app = await NestFactory.createApplicationContext(SyncFromAggregatorDbModule);
  const dataSource = app.get(DataSource);
  const geoLookupService = app.get(GeoLookupService);

  const listingRepository = dataSource.getRepository(UnifiedListing);
  const topzoneRepository = dataSource.getRepository(Topzone);
  const complexRepository = dataSource.getRepository(ApartmentComplex);

  // Load complex matcher for text-based matching
  await complexMatcher.load(dataSource);

  // Check if import table exists
  const tableCheck = await dataSource.query(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'aggregator_import'
    ) as exists
  `);

  if (!tableCheck[0].exists) {
    logger.error('Table aggregator_import does not exist!');
    logger.log('Please run the import first:');
    logger.log('  yarn ts-node scripts/import-aggregator-csv.ts');
    await app.close();
    process.exit(1);
  }

  // Odessa bounding box (approximate)
  const ODESSA_LAT_MIN = 46.3;
  const ODESSA_LAT_MAX = 46.7;
  const ODESSA_LNG_MIN = 30.5;
  const ODESSA_LNG_MAX = 31.0;

  // Get total count (only Odessa region)
  const countResult = await dataSource.query(`
    SELECT COUNT(*) as count FROM aggregator_import
    WHERE lat != '' AND lng != ''
      AND lat::numeric BETWEEN $1 AND $2
      AND lng::numeric BETWEEN $3 AND $4
  `, [ODESSA_LAT_MIN, ODESSA_LAT_MAX, ODESSA_LNG_MIN, ODESSA_LNG_MAX]);
  const totalCount = parseInt(countResult[0].count, 10);
  logger.log(`Total Odessa region records: ${totalCount}`);

  let totalSynced = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let geoResolvedByCoords = 0;
  let streetResolvedByCoords = 0;
  let complexMatchedByText = 0;
  let complexMatchedByCoords = 0;
  let batchNumber = 0;
  let currentOffset = startOffset;

  try {
    while (true) {
      if (maxBatches > 0 && batchNumber >= maxBatches) {
        logger.log(`Reached max batches limit (${maxBatches})`);
        break;
      }

      // Fetch batch from import table (only Odessa region)
      const properties = await dataSource.query<ImportedProperty[]>(`
        SELECT * FROM aggregator_import
        WHERE lat != '' AND lng != ''
          AND lat::numeric BETWEEN $3 AND $4
          AND lng::numeric BETWEEN $5 AND $6
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [batchSize, currentOffset, ODESSA_LAT_MIN, ODESSA_LAT_MAX, ODESSA_LNG_MIN, ODESSA_LNG_MAX]);

      if (properties.length === 0) {
        logger.log('No more records to process');
        break;
      }

      batchNumber++;
      logger.log(`\nBatch ${batchNumber}: Processing ${properties.length} records (offset: ${currentOffset})`);

      // Parallel processing configuration
      const CONCURRENCY = 100; // Process 100 items concurrently

      // Counters for this batch
      let synced = 0;
      let skipped = 0;
      let errors = 0;

      // Pre-fetch all topzones and complexes for this batch to avoid individual queries
      const topzoneIds = [...new Set(properties.map(p => extractInteger(p.topzone_id)).filter(Boolean))];
      const complexIds = [...new Set(properties.map(p => extractInteger(p.complex_id)).filter(Boolean))];

      const [existingTopzones, existingComplexes] = await Promise.all([
        topzoneIds.length > 0 ? topzoneRepository.find({ where: topzoneIds.map(id => ({ id })), select: ['id'] }) : [],
        complexIds.length > 0 ? complexRepository.find({ where: complexIds.map(id => ({ id })), select: ['id'] }) : [],
      ]);

      const validTopzoneIds = new Set(existingTopzones.map(t => t.id));
      const validComplexIds = new Set(existingComplexes.map(c => c.id));

      // Process single item - returns listing data or null
      const processItem = async (item: ImportedProperty): Promise<Partial<UnifiedListing> | null> => {
        try {
          const attributes = parseJsonSafe(item.attributes);
          const primaryData = parseJsonSafe(item.primary_data);
          const lat = extractNumber(item.lat);
          const lng = extractNumber(item.lng);
          const platform = item.realty_platform;

          if (!lat || !lng) return null;

          // Track geo resolution flags
          const geoResolutionFlags: string[] = [];

          let validGeoId: number | undefined = undefined;
          let validStreetId: number | undefined = undefined;
          let validTopzoneId: number | undefined = undefined;
          let validComplexId: number | undefined = undefined;

          const topzoneIdNum = extractInteger(item.topzone_id);
          const complexIdNum = extractInteger(item.complex_id);

          // Use pre-fetched sets instead of individual queries
          validTopzoneId = topzoneIdNum && validTopzoneIds.has(topzoneIdNum) ? topzoneIdNum : undefined;

          // Complex resolution with tracking
          if (complexIdNum && validComplexIds.has(complexIdNum)) {
            validComplexId = complexIdNum;
            geoResolutionFlags.push('complex_from_aggregator');
          }

          // Determine platform from URL
          const externalUrl = item.external_url || '';
          const isOlx = externalUrl.includes('olx.');

          // For OLX: try text-based matching first (synchronous - no DB query)
          if (!validComplexId && isOlx) {
            const urlSlug = externalUrl.split('/').pop()?.replace(/-ID.*$/, '').replace(/-/g, ' ') || '';
            const complexMatch = complexMatcher.findByText(urlSlug, item.description);
            if (complexMatch) {
              validComplexId = complexMatch.id;
              complexMatchedByText++;
              geoResolutionFlags.push('complex_by_text');
            }
          }

          // For ALL platforms: try coordinate-based matching
          if (!validComplexId && lat && lng) {
            const coordMatch = await complexMatcher.findByCoordinates(dataSource, lat, lng);
            if (coordMatch) {
              validComplexId = coordMatch;
              complexMatchedByCoords++;
              geoResolutionFlags.push('complex_by_coords');
            }
          }

          // Prepare text for street matching
          const descriptionText = item.description ? String(item.description) : '';
          const urlText = item.external_url || '';
          const textForMatching = `${descriptionText} ${urlText}`;

          // Resolve geo and street by coordinates + text matching
          const resolved = await geoLookupService.resolveGeoForListingWithText(lng, lat, textForMatching);
          if (resolved.geoId) {
            validGeoId = resolved.geoId;
            geoResolvedByCoords++;
            // Check if it's city-level only (no district)
            if (resolved.isCityLevel) {
              geoResolutionFlags.push('geo_city_level');
            }
          }
          if (resolved.streetId) {
            validStreetId = resolved.streetId;
            streetResolvedByCoords++;
            // Track street match method
            if (resolved.streetMatchMethod === 'text_parsed') {
              geoResolutionFlags.push('street_by_text_parsed');
            } else if (resolved.streetMatchMethod === 'text_found') {
              geoResolutionFlags.push('street_by_text_found');
            } else {
              geoResolutionFlags.push('street_by_nearest');
            }
          } else {
            geoResolutionFlags.push('street_not_found');
          }

          // Track missing topzone
          if (!validTopzoneId) {
            geoResolutionFlags.push('topzone_missing');
          }

          if (!validGeoId) return null;

          const sourceId = extractInteger(item.id);
          if (!sourceId) return null;

          const description = parseJsonSafe(item.description);

          // Resolve condition with fallback tracking
          let conditionUsedFallback = false;
          const condition = (() => {
            // Для realtorUa використовуємо primary_data.main_params.status
            const mainParams = primaryData?.['main_params'] as Record<string, unknown> | undefined;
            if (platform === 'realtorUa' && mainParams?.['status']) {
              const result = mapRealtorUaCondition(mainParams['status'] as string);
              if (result) return result;
            }
            // Для OLX використовуємо primary_data.params де key='repair'
            if (platform === 'olx' && primaryData?.['params']) {
              const olxCondition = mapOlxCondition(primaryData['params']);
              if (olxCondition) return olxCondition;
            }
            // Для domRia використовуємо primary_data.characteristics_values[516]
            if (platform === 'domRia' && primaryData?.['characteristics_values']) {
              const domriaCondition = mapDomriaCondition(primaryData['characteristics_values']);
              if (domriaCondition) return domriaCondition;
            }
            // Fallback - старий маппінг по ID
            conditionUsedFallback = true;
            return mapConditionType(attributes?.condition_type) || (attributes?.condition as string);
          })();
          if (conditionUsedFallback && condition) {
            geoResolutionFlags.push('condition_fallback');
          }

          // Resolve houseType with fallback tracking
          let houseTypeUsedFallback = false;
          const houseType = (() => {
            // Для realtorUa використовуємо primary_data.main_params.border
            const mainParams = primaryData?.['main_params'] as Record<string, unknown> | undefined;
            if (platform === 'realtorUa' && mainParams?.['border']) {
              const result = mapRealtorUaHouseType(mainParams['border'] as string);
              if (result) return result;
            }
            // Для OLX використовуємо primary_data.params де key='property_type_appartments_sale'
            if (platform === 'olx' && primaryData?.['params']) {
              const olxHouseType = mapOlxHouseType(primaryData['params']);
              if (olxHouseType) return olxHouseType;
            }
            // Для domRia використовуємо primary_data.wall_type
            if (platform === 'domRia' && primaryData?.['wall_type']) {
              const domriaHouseType = mapDomriaHouseType(primaryData['wall_type'] as string);
              if (domriaHouseType) return domriaHouseType;
            }
            // Fallback - старий маппінг по ID
            houseTypeUsedFallback = true;
            return mapProjectType(attributes?.project) || (attributes?.houseType as string);
          })();
          if (houseTypeUsedFallback && houseType) {
            geoResolutionFlags.push('house_type_fallback');
          }

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
            geoResolutionFlags: geoResolutionFlags.length > 0 ? geoResolutionFlags : undefined,
          };
        } catch (error) {
          if (errors < 5) {
            logger.error(`Failed to process property ${item.id || 'unknown'}: ${(error as Error).message}`);
          }
          errors++;
          return null;
        }
      };

      // Collect all listings to upsert
      const listingsToUpsert: Partial<UnifiedListing>[] = [];

      // Process in parallel chunks - just prepare data, don't save yet
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

      // Bulk upsert all collected listings
      if (listingsToUpsert.length > 0) {
        try {
          // Use upsert with ON CONFLICT - much faster than individual saves
          // Cast to any to avoid TypeORM's overly strict typing
          await listingRepository.upsert(listingsToUpsert as any, {
            conflictPaths: ['sourceType', 'sourceId'],
            skipUpdateIfNoValuesChanged: true,
          });
          synced = listingsToUpsert.length;
        } catch (error) {
          logger.error(`Bulk upsert failed: ${(error as Error).message}`);
          // Fallback to individual saves
          for (const listing of listingsToUpsert) {
            try {
              await listingRepository.upsert(listing as any, {
                conflictPaths: ['sourceType', 'sourceId'],
              });
              synced++;
            } catch {
              errors++;
            }
          }
        }
      }

      totalSynced += synced;
      totalSkipped += skipped;
      totalErrors += errors;
      currentOffset += batchSize;

      const progress = Math.round((currentOffset / totalCount) * 100);
      logger.log(`Batch ${batchNumber} complete: synced=${synced}, skipped=${skipped}, errors=${errors}`);
      logger.log(`Total progress: ${progress}% (${currentOffset}/${totalCount})`);
      logger.log(`Running totals: synced=${totalSynced}, skipped=${totalSkipped}, errors=${totalErrors}`);
    }

    // Логируем неизвестные ID атрибутов
    logUnknownAttributeIds();

    logger.log('\n' + '='.repeat(60));
    logger.log('Processing completed');
    logger.log('='.repeat(60));
    logger.log(`Total synced: ${totalSynced}`);
    logger.log(`Total skipped: ${totalSkipped}`);
    logger.log(`Total errors: ${totalErrors}`);
    logger.log(`Geo resolved by coordinates: ${geoResolvedByCoords}`);
    logger.log(`Streets resolved by coordinates: ${streetResolvedByCoords}`);
    logger.log(`Complexes matched by text: ${complexMatchedByText}`);
    logger.log(`Complexes matched by coordinates: ${complexMatchedByCoords}`);
    logger.log('='.repeat(60));
  } finally {
    await app.close();
  }
}

main().catch((error) => {
  logger.error('Processing failed:', error);
  process.exit(1);
});
