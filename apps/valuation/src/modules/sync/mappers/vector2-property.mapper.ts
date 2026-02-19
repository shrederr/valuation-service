import { Injectable, Logger } from '@nestjs/common';
import {
  SourceType,
  DealType,
  RealtyType,
  VECTOR2_CONDITION_TYPE_MAP,
  VECTOR2_PROJECT_TYPE_MAP,
  VECTOR2_HOUSING_MATERIAL_MAP,
  VECTOR2_LOCATION_ROOMS_MAP,
  VECTOR2_BALCONY_TYPE_MAP,
  VECTOR2_WINDOWS_KIND_MAP,
  VECTOR2_WINDOWS_FACE_MAP,
  VECTOR2_HEATING_TYPE_MAP,
  VECTOR2_GAS_TYPE_MAP,
  VECTOR2_WATER_TYPE_MAP,
  VECTOR2_ELECTRICITY_TYPE_MAP,
  VECTOR2_SEWERAGE_TYPE_MAP,
  VECTOR2_FURNITURE_MAP,
  VECTOR2_BUILD_TYPE_MAP,
  VECTOR2_OBJECT_TYPE_MAP,
  VECTOR2_SPECIAL_CONDITION_MAP,
  VECTOR2_COMMERCIAL_OBJECT_MAP,
} from '@libs/common';
import { UnifiedListing } from '@libs/database';
import { Vector2ObjectRow } from '../dto';

/**
 * ID resolution maps: vector2 IDs → our local DB IDs
 * Loaded from source_id_mappings table
 */
export interface Vector2IdMappings {
  geo: Map<number, number>;
  street: Map<number, number>;
  complex: Map<number, number>;
}

@Injectable()
export class Vector2PropertyMapper {
  private readonly logger = new Logger(Vector2PropertyMapper.name);

  /**
   * Maps a row from vector2.object table to UnifiedListing
   * @param row — raw vector2 object row
   * @param idMappings — optional ID resolution maps (vector2 → our IDs)
   */
  mapToUnifiedListing(row: Vector2ObjectRow, idMappings?: Vector2IdMappings): Partial<UnifiedListing> {
    const attrs = row.attributes_data || {};
    const realtyType = this.mapRealtyType(row.type_estate, row.fk_subcatid, attrs);
    const dealType = this.mapDealType(row);
    const price = this.getPrice(row);
    const totalArea = this.extractNumber(row.square_total ?? attrs.square_total);
    const livingArea = this.extractNumber(row.square_living ?? attrs.square_living);
    const landArea = this.extractNumber(row.square_land_total ?? attrs.square_land_total);
    const kitchenArea = this.extractNumber(attrs.square_kitchen);
    const lat = this.extractNumber(row.map_x);
    const lng = this.extractNumber(row.map_y);

    // Extract integer attributes
    const rooms = this.extractInteger(attrs.rooms_count);
    const floor = this.extractInteger(attrs.floor);
    const totalFloors = this.extractInteger(attrs.floors_count);

    // Map dictionaries
    const condition = this.mapDict(attrs.condition_type, VECTOR2_CONDITION_TYPE_MAP);
    const houseType = this.mapProjectOrMaterial(attrs.project, attrs.housing_material);
    const planningType = this.mapDict(attrs.location_rooms, VECTOR2_LOCATION_ROOMS_MAP);
    const heatingType = this.mapDict(attrs.heating_type, VECTOR2_HEATING_TYPE_MAP);

    // Currency from currency_json or attributes_data
    const currency = this.extractCurrency(row, attrs);

    // Build enriched attributes for storage
    const enrichedAttributes = this.buildEnrichedAttributes(attrs);

    // Infrastructure — passthrough (format is identical)
    const infrastructure = row.nearest_infrastructure || undefined;

    // Extract nearest distances from infrastructure
    const nearestDistances = this.extractNearestDistances(infrastructure);

    return {
      sourceType: SourceType.VECTOR_CRM,
      sourceId: row.id,
      sourceGlobalId: row.global_id || undefined,
      dealType,
      realtyType,
      geoId: (idMappings?.geo.get(row.fk_geo_id) ?? row.fk_geo_id) || undefined,
      streetId: (row.geo_street && idMappings ? idMappings.street.get(row.geo_street) : row.geo_street) || undefined,
      topzoneId: row.fk_geotop_id || undefined,
      complexId: this.resolveComplexId(attrs.geo_zk, idMappings) || undefined,
      lat: lat ?? undefined,
      lng: lng ?? undefined,
      price: price ?? undefined,
      currency,
      pricePerMeter: totalArea && price ? price / totalArea : undefined,
      totalArea: totalArea ?? undefined,
      livingArea: livingArea ?? undefined,
      kitchenArea: kitchenArea ?? undefined,
      landArea: landArea ?? undefined,
      rooms: rooms ?? undefined,
      floor: floor ?? undefined,
      totalFloors: totalFloors ?? undefined,
      condition: condition || undefined,
      houseType: houseType || undefined,
      planningType: planningType || undefined,
      heatingType: heatingType || undefined,
      attributes: enrichedAttributes,
      infrastructure,
      ...nearestDistances,
      isActive: !row.is_archive,
      isExclusive: false,
      publishedAt: row.time_create ? new Date(row.time_create) : undefined,
      syncedAt: new Date(),
      realtyPlatform: 'vector_crm',
    };
  }

  // =========================================================
  // Realty type determination
  // =========================================================

  /**
   * Maps vector2 type_estate + fk_subcatid + object_type → RealtyType
   *
   * type_estate=1 → apartment
   * type_estate=2:
   *   subcatid=20 → area (land)
   *   subcatid=6  → check object_type: 5=area, 6/7=house
   *   subcatid=7  → house (dacha)
   *   default     → house
   * type_estate=3 → commercial
   */
  private mapRealtyType(
    typeEstate: number,
    subcatid: number,
    attrs: Record<string, unknown>,
  ): RealtyType {
    if (typeEstate === 1) return RealtyType.Apartment;
    if (typeEstate === 3) return RealtyType.Commercial;

    // type_estate=2: houses, land, dachas
    if (typeEstate === 2) {
      if (subcatid === 20) return RealtyType.Area;

      // Check object_type attribute for subcatid=6
      const objectType = this.extractInteger(attrs.object_type);
      if (objectType === 5) return RealtyType.Area; // земля без строений
      if (objectType === 6 || objectType === 7) return RealtyType.House;

      // Default for subcatid=6 (house with land), subcatid=7 (dacha)
      return RealtyType.House;
    }

    return RealtyType.Apartment; // fallback
  }

  // =========================================================
  // Deal type determination
  // =========================================================

  /**
   * Determines deal type from price columns
   * rent_price > 0 → rent, price > 0 → sell
   */
  private mapDealType(row: Vector2ObjectRow): DealType {
    const rentPrice = this.extractNumber(row.rent_price);
    const sellPrice = this.extractNumber(row.price);

    if (rentPrice && rentPrice > 0 && (!sellPrice || sellPrice === 0)) {
      return DealType.Rent;
    }
    return DealType.Sell;
  }

  /**
   * Extracts price: uses rent_price for rent, price for sell
   * Vector2 CRM stores prices in thousands (e.g. 50 = $50,000)
   */
  private getPrice(row: Vector2ObjectRow): number | null {
    const rentPrice = this.extractNumber(row.rent_price);
    const sellPrice = this.extractNumber(row.price);

    let price: number | null;
    if (rentPrice && rentPrice > 0 && (!sellPrice || sellPrice === 0)) {
      price = rentPrice;
    } else {
      price = sellPrice;
    }

    return price ? price * 1000 : price;
  }

  // =========================================================
  // Attribute mapping helpers
  // =========================================================

  /**
   * Maps an integer attribute value via a dictionary
   */
  private mapDict(value: unknown, dict: Record<number, string>): string | undefined {
    const id = this.extractInteger(value);
    if (id === undefined) return undefined;
    return dict[id];
  }

  /**
   * Maps project type with fallback to housing material
   * project → houseType; if no project → housing_material as fallback
   */
  private mapProjectOrMaterial(project: unknown, material: unknown): string | undefined {
    const projectStr = this.mapDict(project, VECTOR2_PROJECT_TYPE_MAP);
    if (projectStr) return projectStr;
    return this.mapDict(material, VECTOR2_HOUSING_MATERIAL_MAP);
  }

  /**
   * Extracts currency from currency_json or attributes_data
   */
  private extractCurrency(row: Vector2ObjectRow, attrs: Record<string, unknown>): string {
    // currency_json may contain {"code": "USD"} or similar
    if (row.currency_json && typeof row.currency_json === 'object') {
      const code = (row.currency_json as Record<string, unknown>).code;
      if (typeof code === 'string') return code.toUpperCase();
    }

    // attributes_data.currency
    const attrCurrency = attrs.currency;
    if (typeof attrCurrency === 'string') {
      const upper = attrCurrency.toUpperCase();
      if (upper === 'USD' || upper === 'EUR' || upper === 'UAH') return upper;
      if (upper === '$' || upper === 'ДОЛ' || upper === 'ДОЛ.') return 'USD';
      if (upper === 'ГРН' || upper === 'ГРН.' || upper === '₴') return 'UAH';
      if (upper === '€' || upper === 'ЄВР') return 'EUR';
    }

    return 'USD'; // default
  }

  // =========================================================
  // Enriched attributes for storage
  // =========================================================

  /**
   * Builds a mapped attributes object from raw integer attributes
   * Stores both raw IDs and resolved string values
   */
  private buildEnrichedAttributes(attrs: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    // Pass through numeric values
    if (attrs.rooms_count !== undefined) result.rooms_count = attrs.rooms_count;
    if (attrs.floor !== undefined) result.floor = attrs.floor;
    if (attrs.floors_count !== undefined) result.floors_count = attrs.floors_count;
    if (attrs.square_total !== undefined) result.square_total = attrs.square_total;
    if (attrs.square_living !== undefined) result.square_living = attrs.square_living;
    if (attrs.square_kitchen !== undefined) result.square_kitchen = attrs.square_kitchen;
    if (attrs.square_land_total !== undefined) result.square_land_total = attrs.square_land_total;
    if (attrs.ceiling_height !== undefined) result.ceiling_height = attrs.ceiling_height;
    if (attrs.description !== undefined) result.description = attrs.description;

    // Map dictionary attributes → resolved strings
    const dictMappings: Array<{
      key: string;
      value: unknown;
      dict: Record<number, string>;
    }> = [
      { key: 'condition_type', value: attrs.condition_type, dict: VECTOR2_CONDITION_TYPE_MAP },
      { key: 'project', value: attrs.project, dict: VECTOR2_PROJECT_TYPE_MAP },
      { key: 'housing_material', value: attrs.housing_material, dict: VECTOR2_HOUSING_MATERIAL_MAP },
      { key: 'location_rooms', value: attrs.location_rooms, dict: VECTOR2_LOCATION_ROOMS_MAP },
      { key: 'balcony_type', value: attrs.balcony_type, dict: VECTOR2_BALCONY_TYPE_MAP },
      { key: 'windows_kind', value: attrs.windows_kind, dict: VECTOR2_WINDOWS_KIND_MAP },
      { key: 'windows_face', value: attrs.windows_face, dict: VECTOR2_WINDOWS_FACE_MAP },
      { key: 'heating_type', value: attrs.heating_type, dict: VECTOR2_HEATING_TYPE_MAP },
      { key: 'gas_type', value: attrs.gas_type, dict: VECTOR2_GAS_TYPE_MAP },
      { key: 'water_type', value: attrs.water_type, dict: VECTOR2_WATER_TYPE_MAP },
      { key: 'electricity_type', value: attrs.electricity_type, dict: VECTOR2_ELECTRICITY_TYPE_MAP },
      { key: 'sewerage_type', value: attrs.sewerage_type, dict: VECTOR2_SEWERAGE_TYPE_MAP },
      { key: 'furniture', value: attrs.furniture, dict: VECTOR2_FURNITURE_MAP },
      { key: 'build_type', value: attrs.build_type, dict: VECTOR2_BUILD_TYPE_MAP },
      { key: 'object_type', value: attrs.object_type, dict: VECTOR2_OBJECT_TYPE_MAP },
      { key: 'special_condition_sale', value: attrs.special_condition_sale, dict: VECTOR2_SPECIAL_CONDITION_MAP },
      { key: 'commercial_object_type', value: attrs.commercial_object_type, dict: VECTOR2_COMMERCIAL_OBJECT_MAP },
    ];

    for (const { key, value, dict } of dictMappings) {
      if (value !== undefined && value !== null) {
        result[key] = value; // raw ID
        const resolved = this.mapDict(value, dict);
        if (resolved) result[`${key}_resolved`] = resolved;
      }
    }

    // Special fields
    if (attrs.bargain !== undefined) result.bargain = attrs.bargain;
    if (attrs.credit_eoselya !== undefined) result.credit_eoselya = attrs.credit_eoselya;
    if (attrs.geo_zk !== undefined) result.geo_zk = attrs.geo_zk;
    if (attrs.method_selling !== undefined) result.method_selling = attrs.method_selling;

    return result;
  }

  // =========================================================
  // Infrastructure distance extraction
  // =========================================================

  private extractNearestDistances(
    infrastructure?: Array<{ type: string; distance: number }>,
  ): Partial<UnifiedListing> {
    if (!infrastructure || !Array.isArray(infrastructure)) return {};

    const result: Partial<UnifiedListing> = {};
    const typeMap: Record<string, keyof Pick<UnifiedListing, 'nearestSchool' | 'nearestHospital' | 'nearestSupermarket' | 'nearestParking' | 'nearestPublicTransport'>> = {
      school: 'nearestSchool',
      hospital: 'nearestHospital',
      supermarket: 'nearestSupermarket',
      parking: 'nearestParking',
      bus_station: 'nearestPublicTransport',
      tram_stop: 'nearestPublicTransport',
      trolleybus_stop: 'nearestPublicTransport',
    };

    for (const item of infrastructure) {
      const field = typeMap[item.type];
      if (field && typeof item.distance === 'number') {
        const current = result[field] as number | undefined;
        if (!current || item.distance < current) {
          (result as Record<string, unknown>)[field] = Math.round(item.distance);
        }
      }
    }

    return result;
  }

  // =========================================================
  // ID resolution helpers
  // =========================================================

  private resolveComplexId(geoZk: unknown, idMappings?: Vector2IdMappings): number | undefined {
    const id = this.extractInteger(geoZk);
    if (!id) return undefined;
    if (idMappings) return idMappings.complex.get(id);
    return id;
  }

  // =========================================================
  // Number extraction utilities
  // =========================================================

  private extractNumber(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return isNaN(value) ? null : value;
    if (typeof value === 'string') {
      const num = parseFloat(value);
      return isNaN(num) ? null : num;
    }
    return null;
  }

  private extractInteger(value: unknown): number | undefined {
    if (value === null || value === undefined) return undefined;
    if (typeof value === 'number') return isNaN(value) ? undefined : Math.floor(value);
    if (typeof value === 'string') {
      const num = parseInt(value, 10);
      return isNaN(num) ? undefined : num;
    }
    return undefined;
  }
}
