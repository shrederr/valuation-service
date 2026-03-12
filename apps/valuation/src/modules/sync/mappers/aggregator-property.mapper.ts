import { Injectable, Logger } from '@nestjs/common';
import { SourceType, DealType, RealtyType, AttributeMapperService } from '@libs/common';
import { UnifiedListing } from '@libs/database';
import { AggregatorPropertyEventDto } from '../dto';
import { GeoLookupService, GeoResolutionResult } from '../../osm/geo-lookup.service';
import { StreetMatcherService } from '../../osm/street-matcher.service';
import { ComplexMatcherService } from '../services/complex-matcher.service';

export interface ComplexMatchInfo {
  complexId: number;
  complexName: string;
  method: 'text' | 'coordinates';
}

export interface MappingResult {
  listing: Partial<UnifiedListing>;
  geoResolution: GeoResolutionResult | null;
  complexMatch: ComplexMatchInfo | null;
  attributeFallbacks: {
    conditionFallback: boolean;
    houseTypeFallback: boolean;
  };
}

@Injectable()
export class AggregatorPropertyMapper {
  private readonly logger = new Logger(AggregatorPropertyMapper.name);

  constructor(
    private readonly geoLookupService: GeoLookupService,
    private readonly streetMatcherService: StreetMatcherService,
    private readonly attributeMapperService: AttributeMapperService,
    private readonly complexMatcherService: ComplexMatcherService,
  ) {}

  /**
   * Maps ExportedProperty from api-property-aggregator to UnifiedListing
   * Now async - uses GeoLookupService for geo/street resolution
   * and AttributeMapperService for condition/houseType mapping
   */
  async mapToUnifiedListing(data: AggregatorPropertyEventDto): Promise<MappingResult> {
    const dealType = this.mapDealType(data.dealType);
    const realtyType = this.mapRealtyType(data.realtyType);
    const attrs = data.attributes || {};

    // Extract values with fallbacks for different naming conventions (snake_case vs camelCase)
    const totalArea = this.extractNumber(attrs.square_total ?? attrs.totalArea);
    const livingArea = this.extractNumber(attrs.square_living ?? attrs.livingArea);
    const kitchenArea = this.extractNumber(attrs.square_kitchen ?? attrs.kitchenArea);
    const rooms = this.extractInt(attrs.rooms_count ?? attrs.rooms);
    const floor = this.extractInt(attrs.floor);
    const totalFloors = this.extractInt(attrs.floors_count ?? attrs.totalFloors);
    const price = data.price;

    // Land area: from attributes (square_land_total is in sotki) or parse from primaryData
    const landAreaSotki = this.extractNumber(attrs.square_land_total ?? attrs.landArea);
    const landAreaFromPrimary = this.extractLandAreaFromPrimaryData(
      data.realtyPlatform, data.primaryData,
    );
    // square_land_total from aggregator is already in sotki, convert to m² for storage
    // extractLandAreaFromPrimaryData already returns m²
    const landArea = landAreaSotki != null ? Math.round(landAreaSotki * 100 * 100) / 100 : landAreaFromPrimary;

    // Price per meter: for area/land use landArea, otherwise totalArea
    const existingPricePerMeter = this.extractNumber(attrs.price_sqr ?? attrs.pricePerMeter);
    const effectiveArea = (realtyType === RealtyType.Area && landArea) ? landArea : totalArea;
    const calculatedPricePerMeter = effectiveArea && price ? Math.round((price / effectiveArea) * 100) / 100 : null;
    const pricePerMeter = existingPricePerMeter ?? calculatedPricePerMeter;

    // Detect platform from realty_platform, URL, or primaryData structure
    const platform = this.attributeMapperService.detectPlatform(
      data.realtyPlatform,
      data.url,
      data.primaryData as Record<string, unknown>,
    );

    const isOlx = platform === 'olx';

    // 1. Try to match apartment complex by text (zkh param + title + description)
    let complexMatch: ComplexMatchInfo | null = null;
    let complexId: number | undefined = undefined;

    try {
      const complexText = this.buildTextForComplexMatching(data);
      if (complexText) {
        const result = await this.complexMatcherService.findComplex(
          complexText,
          data.description?.uk,
          isOlx ? undefined : data.lat, // For OLX: don't use coordinates (approximate)
          isOlx ? undefined : data.lng,
        );

        if (result.complex) {
          complexId = result.complex.id;
          complexMatch = {
            complexId: result.complex.id,
            complexName: result.complex.nameRu || result.complex.nameUk,
            method: result.method as 'text' | 'coordinates',
          };
          this.logger.debug(
            `Complex matched by ${result.method} for property ${data.id}: ${result.complex.nameRu} (id=${result.complex.id})`,
          );
        }
      }
    } catch (err) {
      this.logger.warn(`Complex matching failed for property ${data.id}: ${err instanceof Error ? err.message : err}`);
    }

    // 2. Resolve geo and street
    // ALWAYS resolve geoId from listing coordinates first (don't trust complex or source)
    let geoResolution: GeoResolutionResult | null = null;
    let geoId: number | undefined = undefined;
    let streetId: number | undefined = undefined;

    const complexData = complexMatch ? this.complexMatcherService.getComplexById(complexMatch.complexId) : null;
    const textForMatching = this.buildTextForMatching(data);

    // Step 1: Resolve geo from LISTING coordinates (source of truth)
    // For OLX: only resolve geoId from coordinates, street ONLY from text (coordinates are unreliable)
    if (data.lng && data.lat) {
      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        data.lng,
        data.lat,
        isOlx ? undefined : textForMatching, // OLX: don't use text for coordinate-based street matching
        undefined,
        isOlx, // skipNearestFallback for OLX
      );
      geoId = geoResolution.geoId ?? undefined;
      streetId = isOlx ? undefined : (geoResolution.streetId ?? undefined);
    }

    // For OLX: resolve street by text search within geo polygon (no coordinates)
    if (isOlx && geoId && textForMatching) {
      const textStreetResult = await this.streetMatcherService.resolveStreetByText(textForMatching, geoId);
      if (textStreetResult.streetId) {
        streetId = textStreetResult.streetId;
        this.logger.debug(
          `OLX street resolved by text for property ${data.id}: streetId=${streetId} (${textStreetResult.matchMethod}, confidence=${textStreetResult.confidence.toFixed(2)})`,
        );
      }
    }

    // Step 2: If complex matched, use its street ONLY if same geo (validate distance)
    if (complexData && geoId) {
      if (complexData.streetId && complexData.geoId === geoId) {
        // Complex is in the same geo → trust its street
        streetId = complexData.streetId;
        this.logger.debug(
          `Using complex street for property ${data.id}: streetId=${streetId}, geoId=${geoId}`,
        );
      } else if (complexData.lat && complexData.lng) {
        // Complex has coordinates → check distance before trusting
        const dist = this.haversineDistance(data.lat!, data.lng!, complexData.lat, complexData.lng);
        if (dist > 50) {
          // Complex is > 50km away → false match, discard
          this.logger.warn(
            `Complex match discarded for property ${data.id}: ${complexMatch!.complexName} is ${Math.round(dist)}km away`,
          );
          complexId = undefined;
          complexMatch = null;
        } else {
          // Complex is nearby → use its coords for better street resolution
          const complexGeo = await this.geoLookupService.resolveGeoForListingWithText(
            complexData.lng, complexData.lat, textForMatching,
          );
          // Only use complex street if it resolved to the same geo
          if (complexGeo.geoId === geoId) {
            streetId = complexGeo.streetId ?? streetId;
          }
        }
      }
    } else if (complexData && !geoId && complexData.lat && complexData.lng) {
      // No listing coordinates → resolve from complex coordinates
      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        complexData.lng, complexData.lat, textForMatching,
      );
      geoId = geoResolution.geoId ?? undefined;
      streetId = geoResolution.streetId ?? undefined;
    }

    if (geoResolution?.streetMatchMethod && geoResolution.streetMatchMethod !== 'nearest') {
      this.logger.debug(
        `Street matched by ${geoResolution.streetMatchMethod} for aggregator property ${data.id}: streetId=${streetId}`,
      );
    }

    // Map condition and houseType using platform-specific mappings
    const attributeResult = this.attributeMapperService.mapAttributes(
      platform,
      data.attributes as Record<string, unknown> | undefined,
      data.primaryData,
    );

    const listing: Partial<UnifiedListing> = {
      sourceType: SourceType.AGGREGATOR,
      sourceId: data.id,
      sourceGlobalId: undefined,
      dealType,
      realtyType,
      realtySubtype: undefined,
      geoId: geoId || undefined,
      streetId: streetId || undefined,
      topzoneId: undefined, // Aggregator topzoneId не совпадает с нашей БД
      complexId: complexId || undefined, // Resolved via ComplexMatcherService
      houseNumber: data.houseNumber || undefined,
      apartmentNumber: this.extractInt(attrs.apartmentNumber) ?? undefined,
      corps: (attrs.corps as string) || undefined,
      lat: data.lat || undefined,
      lng: data.lng || undefined,
      price: price || undefined,
      currency: data.currency || 'USD',
      pricePerMeter: pricePerMeter ?? undefined,
      totalArea: totalArea ?? undefined,
      livingArea: livingArea ?? undefined,
      kitchenArea: kitchenArea ?? undefined,
      landArea: landArea ?? undefined,
      rooms: rooms ?? undefined,
      floor: floor ?? undefined,
      totalFloors: totalFloors ?? undefined,
      // Use mapped condition/houseType instead of raw values
      condition: attributeResult.condition || undefined,
      houseType: attributeResult.houseType || undefined,
      planningType: (attrs.planningType as string) || undefined,
      heatingType: (attrs.heatingType as string) || undefined,
      attributes: data.attributes || undefined,
      description: data.description ? { uk: data.description.uk || '' } : undefined,
      isActive: data.isActive ?? true,
      isExclusive: false,
      externalUrl: data.external_url || data.url || undefined,
      publishedAt: data.createdAt ? new Date(data.createdAt) : undefined,
      deletedAt: data.deletedAt ? new Date(data.deletedAt) : undefined,
      syncedAt: new Date(),
      // New fields
      primaryData: data.primaryData || undefined,
      realtyPlatform: data.realtyPlatform || platform || undefined,
      normalizedPhone: this.extractNormalizedPhone(data),
    };

    return {
      listing,
      geoResolution,
      complexMatch,
      attributeFallbacks: {
        conditionFallback: attributeResult.conditionFallback ?? false,
        houseTypeFallback: attributeResult.houseTypeFallback ?? false,
      },
    };
  }

  /**
   * Build text for complex matching: OLX zkh param + title
   */
  private buildTextForComplexMatching(data: AggregatorPropertyEventDto): string {
    const parts: string[] = [];

    if (data.primaryData) {
      const pd = data.primaryData;

      // OLX: extract zkh (apartment complex) param
      if (pd.params && Array.isArray(pd.params)) {
        const zkhParam = (pd.params as Array<Record<string, unknown>>).find(
          (p) => p.key === 'zkh' || p.key === 'complex_name',
        );
        if (zkhParam?.value && typeof zkhParam.value === 'string') {
          parts.push(zkhParam.value);
        }
      }

      // domRia: complex name
      if (pd.complex_name) parts.push(String(pd.complex_name));
      if (pd.building_name) parts.push(String(pd.building_name));

      // realtorUa: complex name
      if (pd.complexName) parts.push(String(pd.complexName));

      // Title often contains ЖК name
      if (pd.title) parts.push(String(pd.title));
    }

    return parts.join(' ');
  }

  /**
   * Build text for street matching from various fields
   */
  private buildTextForMatching(data: AggregatorPropertyEventDto): string {
    const parts: string[] = [];

    // Try to extract street info from primaryData (platform-specific)
    if (data.primaryData) {
      const pd = data.primaryData;

      // domRia: street_name, street_name_uk
      if (pd.street_name_uk) parts.push(String(pd.street_name_uk));
      else if (pd.street_name) parts.push(String(pd.street_name));

      // OLX: location.city, location.district
      if (pd.location && typeof pd.location === 'object') {
        const loc = pd.location as Record<string, unknown>;
        if (loc.pathName) parts.push(String(loc.pathName));
      }

      // realtorUa: address
      if (pd.address) parts.push(String(pd.address));

      // title can contain address info
      if (pd.title) parts.push(String(pd.title));
    }

    // Also use description if available
    if (data.description?.uk) {
      parts.push(data.description.uk);
    }

    return parts.join(' ');
  }

  private mapDealType(dealType: string): DealType {
    const normalized = dealType?.toLowerCase();
    if (normalized === 'rent') {
      return DealType.Rent;
    }
    return DealType.Sell;
  }

  private mapRealtyType(realtyType: string): RealtyType {
    const normalized = realtyType?.toLowerCase();
    const mapping: Record<string, RealtyType> = {
      apartment: RealtyType.Apartment,
      flat: RealtyType.Apartment,
      house: RealtyType.House,
      cottage: RealtyType.House,
      townhouse: RealtyType.House,
      commercial: RealtyType.Commercial,
      office: RealtyType.Commercial,
      retail: RealtyType.Commercial,
      land: RealtyType.Area,
      plot: RealtyType.Area,
      garage: RealtyType.Garage,
      parking: RealtyType.Garage,
      room: RealtyType.Room,
    };
    return mapping[normalized] || RealtyType.Apartment;
  }

  /**
   * Извлекает площадь участка из primaryData (платформо-специфично).
   * Все платформы хранят в сотках → конвертируем в м² (1 сотка = 100 м²).
   */
  private extractLandAreaFromPrimaryData(
    platform: string | undefined,
    primaryData: Record<string, unknown> | undefined,
  ): number | null {
    if (!primaryData) return null;

    let sotki: number | null = null;

    switch (platform) {
      case 'olx': {
        // params[key="land_area"].normalizedValue — "8.1" (сотки)
        const params = primaryData.params;
        if (Array.isArray(params)) {
          const landParam = (params as Array<Record<string, unknown>>).find(
            (p) => p.key === 'land_area',
          );
          if (landParam?.normalizedValue) {
            sotki = this.extractNumber(landParam.normalizedValue);
          }
        }
        break;
      }
      case 'domRia': {
        // ares_count — числовое поле (сотки)
        sotki = this.extractNumber(primaryData.ares_count);
        break;
      }
      case 'realtorUa': {
        // main_params.place_sqr — "4 сот", "14.7 сот" (NOT sqr which is total/living/kitchen area)
        const mainParams = primaryData.main_params as Record<string, unknown> | undefined;
        if (mainParams?.place_sqr && typeof mainParams.place_sqr === 'string') {
          sotki = parseFloat((mainParams.place_sqr as string).replace(/[^\d.,]/g, '').replace(',', '.'));
          if (isNaN(sotki)) sotki = null;
        }
        break;
      }
      case 'realEstateLvivUa': {
        // details["Площа ділянки"] — "6 соток"
        const details = primaryData.details as Record<string, unknown> | undefined;
        const areaStr = details?.['Площа ділянки'];
        if (areaStr && typeof areaStr === 'string') {
          sotki = parseFloat(areaStr.replace(/[^\d.,]/g, '').replace(',', '.'));
          if (isNaN(sotki)) sotki = null;
        }
        break;
      }
      case 'mlsUkraine': {
        // params["ploshcha_zemli_(sotok)"] — "21"
        const params = primaryData.params as Record<string, unknown> | undefined;
        if (params?.['ploshcha_zemli_(sotok)']) {
          sotki = this.extractNumber(params['ploshcha_zemli_(sotok)']);
        }
        break;
      }
    }

    if (sotki && sotki > 0) {
      return Math.round(sotki * 100 * 100) / 100; // сотки → м², округление до 0.01
    }

    return null;
  }

  private extractNormalizedPhone(data: AggregatorPropertyEventDto): string | undefined {
    const seller = data.seller || {};
    const phones = (seller as Record<string, unknown>).phones || (seller as Record<string, unknown>).phone;
    let phone: string | null = null;
    if (Array.isArray(phones) && phones.length > 0) {
      phone = String(phones[0]);
    } else if (typeof phones === 'string') {
      phone = phones;
    }
    if (!phone) return undefined;
    const digits = phone.replace(/\D/g, '');
    if (digits.startsWith('0') && digits.length === 10) return '38' + digits;
    if (digits.startsWith('8') && digits.length === 10) return '38' + digits;
    return digits || undefined;
  }

  private extractNumber(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return isNaN(value) ? null : value;
    if (typeof value === 'string') {
      const num = parseFloat(value);
      return isNaN(num) ? null : num;
    }
    return null;
  }

  private extractInt(value: unknown): number | null {
    const num = this.extractNumber(value);
    return num !== null ? Math.round(num) : null;
  }

  /** Haversine distance in km between two lat/lng points */
  private haversineDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) ** 2 +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
      Math.sin(dLng / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }
}
