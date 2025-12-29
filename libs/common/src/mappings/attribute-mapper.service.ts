import { Injectable, Logger } from '@nestjs/common';
import {
  CONDITION_TYPE_MAP,
  PROJECT_TYPE_MAP,
  REALTOR_UA_STATUS_MAP,
  REALTOR_UA_BORDER_MAP,
  OLX_REPAIR_MAP,
  OLX_PROPERTY_TYPE_MAP,
  DOMRIA_CONDITION_MAP,
  DOMRIA_WALL_TYPE_MAP,
} from './attribute-mappings';

export type RealtyPlatform = 'olx' | 'domRia' | 'realtorUa' | 'rieltor' | 'unknown';

export interface AttributeMapResult {
  condition?: string;
  houseType?: string;
  conditionFallback?: boolean;
  houseTypeFallback?: boolean;
}

@Injectable()
export class AttributeMapperService {
  private readonly logger = new Logger(AttributeMapperService.name);

  /**
   * Detect platform from realty_platform field or URL
   */
  detectPlatform(realtyPlatform?: string, url?: string): RealtyPlatform {
    if (realtyPlatform) {
      const normalized = realtyPlatform.toLowerCase();
      if (normalized.includes('olx')) return 'olx';
      if (normalized.includes('domria') || normalized.includes('dom.ria')) return 'domRia';
      if (normalized.includes('realtor')) return 'realtorUa';
      if (normalized.includes('rieltor')) return 'rieltor';
    }

    if (url) {
      if (url.includes('olx.ua')) return 'olx';
      if (url.includes('dom.ria.com')) return 'domRia';
      if (url.includes('realtor.ua')) return 'realtorUa';
      if (url.includes('rieltor.ua')) return 'rieltor';
    }

    return 'unknown';
  }

  /**
   * Map condition from attributes and/or primary_data based on platform
   */
  mapCondition(
    platform: RealtyPlatform,
    attributes?: Record<string, unknown>,
    primaryData?: Record<string, unknown>,
  ): { value?: string; fallback: boolean } {
    // 1. Try platform-specific mapping from primary_data
    if (primaryData) {
      const result = this.mapConditionByPlatform(platform, primaryData);
      if (result) return { value: result, fallback: false };
    }

    // 2. Try condition_type from attributes (vector-api style IDs)
    if (attributes?.condition_type) {
      const id = this.extractInteger(attributes.condition_type);
      if (id && CONDITION_TYPE_MAP[id]) {
        return { value: CONDITION_TYPE_MAP[id], fallback: false };
      }
    }

    // 3. Fallback to raw condition string
    if (attributes?.condition && typeof attributes.condition === 'string') {
      return { value: attributes.condition, fallback: true };
    }

    return { value: undefined, fallback: false };
  }

  /**
   * Map houseType from attributes and/or primary_data based on platform
   */
  mapHouseType(
    platform: RealtyPlatform,
    attributes?: Record<string, unknown>,
    primaryData?: Record<string, unknown>,
  ): { value?: string; fallback: boolean } {
    // 1. Try platform-specific mapping from primary_data
    if (primaryData) {
      const result = this.mapHouseTypeByPlatform(platform, primaryData);
      if (result) return { value: result, fallback: false };
    }

    // 2. Try project from attributes (vector-api style IDs)
    if (attributes?.project) {
      const id = this.extractInteger(attributes.project);
      if (id && PROJECT_TYPE_MAP[id]) {
        return { value: PROJECT_TYPE_MAP[id], fallback: false };
      }
    }

    // 3. Fallback to raw houseType string
    if (attributes?.houseType && typeof attributes.houseType === 'string') {
      return { value: attributes.houseType, fallback: true };
    }

    return { value: undefined, fallback: false };
  }

  /**
   * Map all attributes at once
   */
  mapAttributes(
    platform: RealtyPlatform,
    attributes?: Record<string, unknown>,
    primaryData?: Record<string, unknown>,
  ): AttributeMapResult {
    const conditionResult = this.mapCondition(platform, attributes, primaryData);
    const houseTypeResult = this.mapHouseType(platform, attributes, primaryData);

    return {
      condition: conditionResult.value,
      houseType: houseTypeResult.value,
      conditionFallback: conditionResult.fallback,
      houseTypeFallback: houseTypeResult.fallback,
    };
  }

  // ============================================================
  // Platform-specific mapping methods
  // ============================================================

  private mapConditionByPlatform(
    platform: RealtyPlatform,
    primaryData: Record<string, unknown>,
  ): string | undefined {
    switch (platform) {
      case 'olx':
        return this.mapOlxCondition(primaryData);
      case 'domRia':
        return this.mapDomriaCondition(primaryData);
      case 'realtorUa':
        return this.mapRealtorUaCondition(primaryData);
      default:
        return undefined;
    }
  }

  private mapHouseTypeByPlatform(
    platform: RealtyPlatform,
    primaryData: Record<string, unknown>,
  ): string | undefined {
    switch (platform) {
      case 'olx':
        return this.mapOlxHouseType(primaryData);
      case 'domRia':
        return this.mapDomriaHouseType(primaryData);
      case 'realtorUa':
        return this.mapRealtorUaHouseType(primaryData);
      default:
        return undefined;
    }
  }

  // ============================================================
  // OLX
  // ============================================================

  private mapOlxCondition(primaryData: Record<string, unknown>): string | undefined {
    const params = primaryData.params;
    if (!Array.isArray(params)) return undefined;

    // Check 'repair' key
    const repairParam = params.find((p: any) => p?.key === 'repair');
    if (repairParam?.value) {
      const result = OLX_REPAIR_MAP[repairParam.value as string];
      if (result) return result;
    }

    // Check 'is_repaired' for commercial
    const isRepairedParam = params.find((p: any) => p?.key === 'is_repaired');
    if (isRepairedParam?.value) {
      const val = isRepairedParam.value as string;
      if (val === 'Так' || val === 'yes') return 'Євроремонт';
      if (val === 'Ні' || val === 'no') return 'Без ремонту';
    }

    return undefined;
  }

  private mapOlxHouseType(primaryData: Record<string, unknown>): string | undefined {
    const params = primaryData.params;
    if (!Array.isArray(params)) return undefined;

    const propertyTypeParam = params.find((p: any) => p?.key === 'property_type_appartments_sale');
    if (propertyTypeParam?.value) {
      return OLX_PROPERTY_TYPE_MAP[propertyTypeParam.value as string];
    }

    return undefined;
  }

  // ============================================================
  // domRia
  // ============================================================

  private mapDomriaCondition(primaryData: Record<string, unknown>): string | undefined {
    const charValues = primaryData.characteristics_values as Record<string, unknown> | undefined;
    if (!charValues) return undefined;

    const value = this.extractInteger(charValues['516']);
    if (value && DOMRIA_CONDITION_MAP[value]) {
      return DOMRIA_CONDITION_MAP[value];
    }

    return undefined;
  }

  private mapDomriaHouseType(primaryData: Record<string, unknown>): string | undefined {
    const wallType = primaryData.wall_type as string | undefined;
    if (!wallType) return undefined;

    return DOMRIA_WALL_TYPE_MAP[wallType.toLowerCase()];
  }

  // ============================================================
  // realtorUa
  // ============================================================

  private mapRealtorUaCondition(primaryData: Record<string, unknown>): string | undefined {
    const mainParams = primaryData.main_params as Record<string, unknown> | undefined;
    if (!mainParams) return undefined;

    const status = mainParams.status as string | undefined;
    if (status) {
      return REALTOR_UA_STATUS_MAP[status];
    }

    return undefined;
  }

  private mapRealtorUaHouseType(primaryData: Record<string, unknown>): string | undefined {
    const mainParams = primaryData.main_params as Record<string, unknown> | undefined;
    if (!mainParams) return undefined;

    const border = mainParams.border as string | undefined;
    if (border) {
      return REALTOR_UA_BORDER_MAP[border];
    }

    return undefined;
  }

  // ============================================================
  // Helpers
  // ============================================================

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
