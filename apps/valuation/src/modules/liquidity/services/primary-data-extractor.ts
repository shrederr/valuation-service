import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

import {
  DOMRIA_CONDITION_MAP,
  VECTOR_CONDITION_TYPE_MAP,
  REALTOR_CONDITION_SCORE,
  REAL_ESTATE_CONDITION_SCORE,
  WALL_TYPE_SCORE,
  LAYOUT_TYPE_SCORE,
  OLX_COMFORT_NORMALIZE,
} from '../constants/platform-mappings';

/**
 * Універсальний парсер primaryData для різних платформ.
 *
 * Платформи визначаються по полю listing.realtyPlatform:
 *   olx (482k): params: [{key, value}] — масив
 *   domRia (274k): characteristics_values: {id: value}, wall_type, wall_type_uk
 *   realtorUa (263k): main_params: {}, addition_params: []
 *   realEstateLvivUa (72k): details: {"Ключ": "значення"}
 *   mlsUkraine (5k): params: {key: value} — об'єкт
 *   vector_crm (394k): attributes (entity field), немає primaryData
 */
@Injectable()
export class PrimaryDataExtractor {

  // ─── Platform Detection ─────────────────────────────────────────────

  private getPlatform(listing: UnifiedListing): string {
    return listing.realtyPlatform || 'unknown';
  }

  // ─── Platform-specific Raw Accessors ────────────────────────────────

  /** OLX: params: [{key, value}] — масив */
  private getOlxParam(pd: Record<string, unknown> | undefined, key: string): string | null {
    if (!pd) return null;
    const params = pd.params;
    if (!Array.isArray(params)) return null;

    const param = params.find((p: Record<string, unknown>) => p && p.key === key);
    if (!param) return null;

    const value = (param as Record<string, unknown>).value ?? (param as Record<string, unknown>).normalizedValue;
    return typeof value === 'string' ? value : null;
  }

  /** domRia: characteristics_values: {charId: value} */
  private getDomRiaCharValue(pd: Record<string, unknown> | undefined, charId: string): string | null {
    if (!pd) return null;
    const cv = pd.characteristics_values;
    if (!cv || typeof cv !== 'object') return null;
    const value = (cv as Record<string, unknown>)[charId];
    if (value === undefined || value === null) return null;
    return String(value);
  }

  /** realtorUa: main_params: {key: value} */
  private getRealtorUaMainParam(pd: Record<string, unknown> | undefined, key: string): string | null {
    if (!pd) return null;
    const mainParams = pd.main_params;
    if (!mainParams || typeof mainParams !== 'object') return null;
    const value = (mainParams as Record<string, unknown>)[key];
    return typeof value === 'string' && value.trim() ? value.trim() : null;
  }

  /** realtorUa: addition_params: string[] */
  private getRealtorUaAdditionParams(pd: Record<string, unknown> | undefined): string[] | null {
    if (!pd) return null;
    const addParams = pd.addition_params;
    if (!Array.isArray(addParams)) return null;
    const strings = addParams.filter((v): v is string => typeof v === 'string');
    return strings.length > 0 ? strings : null;
  }

  /** realEstateLvivUa: details: {"Ключ": "значення"} */
  private getRealEstateLvivDetail(pd: Record<string, unknown> | undefined, key: string): string | null {
    if (!pd) return null;
    const details = pd.details;
    if (!details || typeof details !== 'object') return null;
    const value = (details as Record<string, unknown>)[key];
    return typeof value === 'string' && value.trim() ? value.trim() : null;
  }

  /** mlsUkraine: params: {key: value} — об'єкт (не масив) */
  private getMlsParam(pd: Record<string, unknown> | undefined, key: string): string | null {
    if (!pd) return null;
    const params = pd.params;
    if (!params || typeof params !== 'object' || Array.isArray(params)) return null;
    const value = (params as Record<string, unknown>)[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
    if (typeof value === 'boolean') return value ? 'так' : 'ні';
    return null;
  }

  /** mlsUkraine: params as Record */
  private getMlsParams(pd: Record<string, unknown> | undefined): Record<string, unknown> | null {
    if (!pd) return null;
    const params = pd.params;
    if (!params || typeof params !== 'object' || Array.isArray(params)) return null;
    return params as Record<string, unknown>;
  }

  // ─── Condition / Стан ──────────────────────────────────────────────

  /**
   * Витягує стан/ремонт об'єкта.
   * Повертає: { text: string; score?: number } | null
   * score присутній коли маппінг однозначний (domRia, vector_crm)
   */
  public extractCondition(listing: UnifiedListing): { text: string; score?: number } | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const value = this.getOlxParam(pd, 'repair');
        if (value) return { text: value };
        break;
      }

      case 'domRia': {
        // characteristics_values['516'] = код стану
        const charValue = this.getDomRiaCharValue(pd, '516');
        if (charValue) {
          const mapped = DOMRIA_CONDITION_MAP[charValue];
          if (mapped) return { text: mapped.text, score: mapped.score };
          return { text: `domRia код ${charValue}` };
        }
        break;
      }

      case 'realtorUa': {
        const status = this.getRealtorUaMainParam(pd, 'status');
        if (status) {
          const score = REALTOR_CONDITION_SCORE[status.toLowerCase()];
          if (score !== undefined) return { text: status, score };
          return { text: status };
        }
        break;
      }

      case 'realEstateLvivUa': {
        const condition = this.getRealEstateLvivDetail(pd, 'Стан');
        if (condition) {
          const score = REAL_ESTATE_CONDITION_SCORE[condition.toLowerCase()];
          if (score !== undefined) return { text: condition, score };
          return { text: condition };
        }
        break;
      }

      case 'mlsUkraine': {
        const value = this.getMlsParam(pd, 'stan(pemont)_obyekta');
        if (value) return { text: value };
        break;
      }

      case 'vector_crm': {
        const attrs = listing.attributes;
        if (attrs?.condition_type !== undefined) {
          const code = Number(attrs.condition_type);
          const mapped = VECTOR_CONDITION_TYPE_MAP[code];
          if (mapped) return { text: mapped.text, score: mapped.score };
        }
        break;
      }
    }

    // Fallback: entity колонка condition (заповнена AttributeMapperService)
    if (listing.condition) return { text: listing.condition };

    return null;
  }

  // ─── Furnish / Меблі ──────────────────────────────────────────────

  public extractFurnish(listing: UnifiedListing): 'yes' | 'no' | 'partial' | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const value = this.getOlxParam(pd, 'furnish');
        if (value) return this.parseFurnishValue(value);
        break;
      }

      case 'mlsUkraine': {
        const value = this.getMlsParam(pd, 'mebli');
        if (value) return this.parseFurnishValue(value);
        break;
      }

      case 'vector_crm': {
        const attrs = listing.attributes;
        if (attrs?.furniture !== undefined) {
          const furn = Number(attrs.furniture);
          if (furn === 1) return 'yes';
          if (furn === 2) return 'no';
        }
        // rent_furniture for rent
        if (attrs?.rent_furniture !== undefined) {
          const rf = Number(attrs.rent_furniture);
          if (rf === 1) return 'yes';
          if (rf === 2) return 'partial';
          if (rf === 3) return 'no';
        }
        break;
      }

      // domRia, realtorUa, realEstateLvivUa — немає даних про меблі
      default:
        break;
    }

    return null;
  }

  private parseFurnishValue(value: string): 'yes' | 'no' | 'partial' | null {
    const lower = value.toLowerCase();
    if (lower === 'так' || lower === 'yes' || lower === 'да') return 'yes';
    if (lower === 'ні' || lower === 'no' || lower === 'нет') return 'no';
    if (lower === 'частково' || lower === 'partial' || lower === 'частично') return 'partial';
    return null;
  }

  // ─── Communications / Комунікації ──────────────────────────────────

  public extractCommunications(listing: UnifiedListing): string[] | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const commValue = this.getOlxParam(pd, 'communications');
        if (commValue) {
          const items = commValue.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
          this.enrichFromOlxParams(pd, items);
          if (items.length > 0) return items;
        }
        // Якщо немає — enrichFromOlxParams alone
        const items: string[] = [];
        this.enrichFromOlxParams(pd, items);
        if (items.length > 0) return items;
        break;
      }

      case 'domRia': {
        // domRia не має структурованих даних комунікацій в characteristics_values
        // Проте для квартир застосовується apartment default в criterion
        break;
      }

      case 'realtorUa': {
        // Немає структурованих даних — apartment default в criterion
        break;
      }

      case 'realEstateLvivUa': {
        // Немає структурованих даних — apartment default в criterion
        break;
      }

      case 'mlsUkraine':
        return this.extractMlsCommunications(pd);

      case 'vector_crm':
        return this.extractVector2Communications(listing.attributes);
    }

    return null;
  }

  private enrichFromOlxParams(pd: Record<string, unknown> | undefined, items: string[]): void {
    const heatingValue = this.getOlxParam(pd, 'heating');
    if (heatingValue) {
      const lower = heatingValue.toLowerCase();
      if (!lower.includes('відсутн') && !lower.includes('без ')) {
        if (!items.some(i => i.includes('опален') || i.includes('heat'))) {
          items.push('heating');
        }
        if (lower.includes('газов')) {
          if (!items.some(i => i.includes('газ') || i.includes('gas'))) {
            items.push('gas');
          }
        }
      }
    }

    const bathroomValue = this.getOlxParam(pd, 'bathroom');
    if (bathroomValue) {
      const lower = bathroomValue.toLowerCase();
      if (!lower.includes('відсутн')) {
        if (!items.some(i => i.includes('вод') || i.includes('water'))) {
          items.push('water');
        }
        if (!items.some(i => i.includes('каналіз') || i.includes('sewer'))) {
          items.push('sewerage');
        }
      }
    }
  }

  private extractMlsCommunications(pd: Record<string, unknown> | undefined): string[] | null {
    if (!pd) return null;
    const params = pd.params;
    if (!params || typeof params !== 'object' || Array.isArray(params)) return null;
    const p = params as Record<string, unknown>;

    const comms: string[] = [];
    const val = (key: string) => {
      const v = p[key];
      return typeof v === 'string' ? v.toLowerCase() : v === true ? 'так' : null;
    };

    const elektryka = val('elektryka');
    if (elektryka && elektryka !== 'ні') comms.push('electricity');

    const voda = val('voda');
    if (voda && voda !== 'ні' && !voda.includes('відсутн')) comms.push('water');

    const haz = val('haz');
    if (haz && haz !== 'ні') comms.push('gas');

    const opalennja = val('opalennja');
    if (opalennja && opalennja !== 'ні' && !opalennja.includes('відсутн')) comms.push('heating');

    const internetTv = val('internet_tv');
    if (internetTv && internetTv !== 'ні') comms.push('internet');

    const sanvuzol = val('typ_canvuzla(iv)');
    if (sanvuzol && !sanvuzol.includes('відсутн')) comms.push('sewerage');

    return comms.length > 0 ? comms : null;
  }

  private extractVector2Communications(attrs: Record<string, unknown> | undefined): string[] | null {
    if (!attrs) return null;

    const comms: string[] = [];
    if (attrs.electricity_type !== undefined && Number(attrs.electricity_type) !== 3) {
      comms.push('electricity');
    }
    const waterType = Number(attrs.water_type);
    if (attrs.water_type !== undefined && waterType !== 5 && waterType !== 13) {
      comms.push('water');
    }
    if (attrs.gas_type !== undefined && Number(attrs.gas_type) !== 3) {
      comms.push('gas');
    }
    if (attrs.heating_type !== undefined && Number(attrs.heating_type) !== 6) {
      comms.push('heating');
    }
    if (attrs.sewerage_type !== undefined && Number(attrs.sewerage_type) !== 3) {
      comms.push('sewerage');
    }

    return comms.length > 0 ? comms : null;
  }

  // ─── Comfort / Комфорт ────────────────────────────────────────────

  public extractComfort(listing: UnifiedListing): string[] | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const olxValue = this.getOlxParam(pd, 'comfort');
        if (olxValue) {
          const rawItems = olxValue.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
          // Нормалізуємо OLX comfort теги
          const normalized = rawItems.map(item => OLX_COMFORT_NORMALIZE[item] || item);
          if (normalized.length > 0) return normalized;
        }
        break;
      }

      case 'mlsUkraine':
        return this.extractMlsComfort(pd);

      case 'vector_crm':
        return this.extractVector2Comfort(listing.attributes);

      // domRia, realtorUa, realEstateLvivUa — немає структурованих даних комфорту
      default:
        break;
    }

    return null;
  }

  private extractMlsComfort(pd: Record<string, unknown> | undefined): string[] | null {
    if (!pd) return null;
    const params = pd.params;
    if (!params || typeof params !== 'object' || Array.isArray(params)) return null;
    const p = params as Record<string, unknown>;

    const comfort: string[] = [];
    const isYes = (key: string) => {
      const v = p[key];
      return (typeof v === 'string' && v.toLowerCase() === 'так') || v === true;
    };

    if (isYes('lift')) comfort.push('elevator');
    if (isYes('balkon_lodzhija')) comfort.push('balcony');
    if (isYes('harazh')) comfort.push('parking');
    if (isYes('kondytsionuvannja_')) comfort.push('conditioner');
    if (isYes('kamin')) comfort.push('fireplace');

    return comfort.length > 0 ? comfort : null;
  }

  private extractVector2Comfort(attrs: Record<string, unknown> | undefined): string[] | null {
    if (!attrs) return null;

    const comfort: string[] = [];
    const balcony = Number(attrs.balcony_type);
    if (balcony >= 2) {
      if (balcony === 4 || balcony === 5 || balcony === 11 || balcony === 15) {
        comfort.push('loggia');
      } else {
        comfort.push('balcony');
      }
    }
    const windowsFace = Number(attrs.windows_face);
    if (windowsFace === 9 || windowsFace === 11) {
      comfort.push('panoramic_windows');
    }
    if (attrs.parking !== undefined && Number(attrs.parking) >= 2) {
      comfort.push('parking');
    }
    if (attrs.elevator_count !== undefined && Number(attrs.elevator_count) > 0) {
      comfort.push('elevator');
    }

    return comfort.length > 0 ? comfort : null;
  }

  // ─── Layout / Планування ──────────────────────────────────────────

  /**
   * Витягує тип планування.
   * Повертає: { text: string; score?: number } | null
   */
  public extractLayout(listing: UnifiedListing): { text: string; score?: number } | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const value = this.getOlxParam(pd, 'layout');
        if (value) {
          const score = LAYOUT_TYPE_SCORE[value.toLowerCase()];
          return { text: value, score: score !== undefined ? score : undefined };
        }
        break;
      }

      case 'realtorUa': {
        const value = this.getRealtorUaMainParam(pd, 'planirovka');
        if (value) {
          const score = LAYOUT_TYPE_SCORE[value.toLowerCase()];
          return { text: value, score: score !== undefined ? score : undefined };
        }
        break;
      }

      case 'mlsUkraine': {
        const value = this.getMlsParam(pd, 'osoblyvosti_planuvannja');
        if (value) {
          const score = LAYOUT_TYPE_SCORE[value.toLowerCase()];
          return { text: value, score: score !== undefined ? score : undefined };
        }
        break;
      }

      case 'vector_crm': {
        const attrs = listing.attributes;
        if (attrs?.location_rooms) {
          const value = String(attrs.location_rooms);
          const score = LAYOUT_TYPE_SCORE[value.toLowerCase()];
          return { text: value, score: score !== undefined ? score : undefined };
        }
        break;
      }

      // domRia, realEstateLvivUa — немає даних планування
      default:
        break;
    }

    // Fallback: entity колонка planningType
    if (listing.planningType) {
      const score = LAYOUT_TYPE_SCORE[listing.planningType.toLowerCase()];
      return { text: listing.planningType, score: score !== undefined ? score : undefined };
    }

    return null;
  }

  // ─── House Type / Тип будинку ─────────────────────────────────────

  /**
   * Витягує тип будинку / стін.
   * Повертає: { text: string; score?: number } | null
   */
  public extractHouseType(listing: UnifiedListing): { text: string; score?: number } | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx': {
        const value = this.getOlxParam(pd, 'house_type');
        if (value) return { text: value };
        break;
      }

      case 'domRia': {
        // wall_type_uk або wall_type
        const wallType = pd?.wall_type_uk || pd?.wall_type;
        if (wallType && typeof wallType === 'string') {
          const lower = wallType.toLowerCase().trim();
          const score = WALL_TYPE_SCORE[lower];
          return { text: wallType, score: score !== undefined ? score : undefined };
        }
        break;
      }

      case 'realtorUa': {
        const border = this.getRealtorUaMainParam(pd, 'border');
        if (border) return { text: border };
        break;
      }

      case 'realEstateLvivUa': {
        const value = this.getRealEstateLvivDetail(pd, 'Матеріал стін');
        if (value) return { text: value };
        break;
      }

      case 'mlsUkraine': {
        const value = this.getMlsParam(pd, 'typ_stin');
        if (value) return { text: value };
        break;
      }

      case 'vector_crm': {
        // vector_crm заповнює entity колонку houseType при синхронізації
        break;
      }
    }

    // Fallback: entity колонка houseType
    if (listing.houseType) return { text: listing.houseType };

    return null;
  }

  // ─── Buy Conditions / Умови купівлі ───────────────────────────────

  public extractBuyConditions(listing: UnifiedListing): string[] | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;
    const conditions: string[] = [];

    switch (platform) {
      case 'vector_crm': {
        const attrs = listing.attributes;
        if (attrs) {
          if (Number(attrs.credit_eoselya) === 1 || Number(attrs.credit_eoselya_2) === 1) conditions.push('eOselya');
          if (Number(attrs.credit_evidnovlenya) === 1) conditions.push('eVidnovlennya');
          if (Number(attrs.credit_dmj) === 1) conditions.push('dmzh');
          if (Number(attrs.in_installments) === 1) conditions.push('installment');
          if (Number(attrs.bargain) === 1) conditions.push('bargain');
          if (Number(attrs.method_selling) === 2) conditions.push('assignment');
          // commision_ssum: 0 або null = без комісії
          if (attrs.commision_ssum !== undefined && (Number(attrs.commision_ssum) === 0 || attrs.commision_ssum === null)) {
            conditions.push('noCommission');
          }
          // special_condition_sale
          const scs = Number(attrs.special_condition_sale);
          if (scs === 1 && !conditions.includes('mortgage')) conditions.push('mortgage');
        }
        break;
      }

      case 'olx': {
        // OLX: params eoselia, cooperate
        const eoselia = this.getOlxParam(pd, 'eoselia');
        if (eoselia && eoselia.toLowerCase() !== 'ні') conditions.push('eOselya');
        break;
      }

      case 'domRia': {
        // characteristics: 274=розстрочка/кредит, 1437=тип пропозиції, 273=торг, 265=обмін
        const installment = this.getDomRiaCharValue(pd, '274');
        if (installment && installment !== '0') conditions.push('installment');

        const bargain = this.getDomRiaCharValue(pd, '273');
        if (bargain && bargain !== '0') conditions.push('bargain');

        const exchange = this.getDomRiaCharValue(pd, '265');
        if (exchange && exchange !== '0') conditions.push('exchange');

        // 1437=тип пропозиції (потребує уточнення кодів)
        break;
      }

      case 'realtorUa': {
        const addParams = this.getRealtorUaAdditionParams(pd);
        if (addParams) {
          const joined = addParams.join(' ').toLowerCase();
          if (joined.includes('без комісі')) conditions.push('noCommission');
          if (joined.includes('переуступ')) conditions.push('assignment');
          if (joined.includes('єоселя')) conditions.push('eOselya');
          if (joined.includes('євідновлення') || joined.includes('відновлення')) conditions.push('eVidnovlennya');
        }
        break;
      }

      case 'mlsUkraine': {
        const eOselya = this.getMlsParam(pd, 'e_oselya');
        if (eOselya && eOselya.toLowerCase() === 'так') conditions.push('eOselya');
        const umova = this.getMlsParam(pd, 'umova_prodazhu');
        if (umova) {
          const lower = umova.toLowerCase();
          if (lower.includes('розстрочк') || lower.includes('розтерміновк')) conditions.push('installment');
          if (lower.includes('торг')) conditions.push('bargain');
        }
        break;
      }

      // realEstateLvivUa — немає даних
      default:
        break;
    }

    // Text search in description (для всіх платформ — доповнення)
    const description = this.getDescriptionText(listing);
    const attrs = listing.attributes;
    const attrDesc = attrs?.description as string | undefined;
    const attrDescRekl = attrs?.description_rekl_ua as string | undefined;
    const allText = [description, attrDesc, attrDescRekl].filter(Boolean).join(' ').toLowerCase();

    if (allText) {
      // Word boundary regex — уникаємо хибних спрацювань
      if (!conditions.includes('eOselya') && /\bєоселя\b/i.test(allText)) {
        conditions.push('eOselya');
      }
      if (!conditions.includes('eVidnovlennya') && /\bєвідновлення\b/i.test(allText)) {
        conditions.push('eVidnovlennya');
      }
      if (!conditions.includes('dmzh') && (/\bдмж\b/i.test(allText) || /державна молодіжна/i.test(allText))) {
        conditions.push('dmzh');
      }
      if (!conditions.includes('mortgage') && /\b(іпотек|ипотек)/i.test(allText)) {
        conditions.push('mortgage');
      }
      if (!conditions.includes('installment') && /\b(розтерміновк|розстрочк|рассрочк)/i.test(allText)) {
        conditions.push('installment');
      }
      if (!conditions.includes('bargain') && /\bторг\b/i.test(allText)) {
        conditions.push('bargain');
      }
      if (!conditions.includes('exchange') && /\bобмін\b/i.test(allText)) {
        conditions.push('exchange');
      }
      if (!conditions.includes('assignment') && /\b(переуступ|цесі)/i.test(allText)) {
        conditions.push('assignment');
      }
      if (!conditions.includes('noCommission') && /без комісі/i.test(allText)) {
        conditions.push('noCommission');
      }
    }

    return conditions.length > 0 ? conditions : null;
  }

  // ─── Description Text ─────────────────────────────────────────────

  public getDescriptionText(listing: UnifiedListing): string | null {
    const platform = this.getPlatform(listing);
    const pd = listing.primaryData;

    switch (platform) {
      case 'olx':
        if (pd?.description && typeof pd.description === 'string') return pd.description;
        break;

      case 'domRia':
        // domRia має description_uk
        if (pd?.description_uk && typeof pd.description_uk === 'string') return pd.description_uk;
        if (pd?.description && typeof pd.description === 'string') return pd.description;
        break;

      case 'realtorUa':
        if (pd?.description && typeof pd.description === 'string') return pd.description;
        break;

      case 'realEstateLvivUa':
        if (pd?.description && typeof pd.description === 'string') return pd.description;
        break;

      case 'mlsUkraine':
        if (pd?.descript && typeof pd.descript === 'string') return pd.descript;
        break;

      case 'vector_crm': {
        const attrs = listing.attributes;
        if (attrs?.description_rekl_ua && typeof attrs.description_rekl_ua === 'string') {
          return attrs.description_rekl_ua;
        }
        if (attrs?.description && typeof attrs.description === 'string') {
          return attrs.description;
        }
        break;
      }
    }

    // Fallback: entity field
    if (!listing.description) return null;
    if (typeof listing.description === 'string') return listing.description;

    const desc = listing.description as unknown as { uk?: string; ru?: string };
    return desc.uk || desc.ru || null;
  }
}
