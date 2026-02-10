import { Injectable, Logger } from '@nestjs/common';
import { SourceType, DealType, RealtyType, AttributeMapperService } from '@libs/common';
import { UnifiedListing } from '@libs/database';
import { AggregatorPropertyEventDto } from '../dto';
import { GeoLookupService, GeoResolutionResult } from '../../osm/geo-lookup.service';
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
    const rooms = this.extractNumber(attrs.rooms_count ?? attrs.rooms);
    const floor = this.extractNumber(attrs.floor);
    const totalFloors = this.extractNumber(attrs.floors_count ?? attrs.totalFloors);
    const price = data.price;

    // Price per meter: use existing value or calculate
    const existingPricePerMeter = this.extractNumber(attrs.price_sqr ?? attrs.pricePerMeter);
    const calculatedPricePerMeter = totalArea && price ? price / totalArea : null;
    const pricePerMeter = existingPricePerMeter ?? calculatedPricePerMeter;

    // Detect platform from realty_platform or URL
    const platform = this.attributeMapperService.detectPlatform(
      data.realtyPlatform,
      data.url,
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
    let geoResolution: GeoResolutionResult | null = null;
    let geoId: number | undefined = undefined;
    let streetId: number | undefined = undefined;

    // If complex found with known coordinates → use complex coordinates for street resolution (more accurate)
    const complexData = complexMatch ? this.complexMatcherService.getComplexById(complexMatch.complexId) : null;

    if (complexData?.streetId) {
      // Complex has a known street → use it directly
      streetId = complexData.streetId;
      geoId = complexData.geoId;

      // Still resolve geoId from coordinates if complex doesn't have one
      if (!geoId && data.lng && data.lat) {
        geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
          data.lng, data.lat, undefined, data.geoId,
        );
        geoId = geoResolution.geoId ?? undefined;
      }

      this.logger.debug(
        `Using complex street for property ${data.id}: streetId=${streetId}, geoId=${geoId}`,
      );
    } else if (complexData?.lat && complexData?.lng) {
      // Complex has accurate coordinates → use them for street resolution
      const textForMatching = this.buildTextForMatching(data);
      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        complexData.lng,
        complexData.lat,
        textForMatching,
        data.geoId,
      );

      geoId = geoResolution.geoId ?? undefined;
      streetId = geoResolution.streetId ?? undefined;

      this.logger.debug(
        `Using complex coords for property ${data.id}: geoId=${geoId}, streetId=${streetId}`,
      );
    } else if (data.lng && data.lat) {
      // No complex → resolve by listing coordinates
      const textForMatching = this.buildTextForMatching(data);

      // For OLX: skip nearest street fallback (coordinates are approximate)
      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        data.lng,
        data.lat,
        textForMatching,
        data.geoId,
        isOlx, // skipNearestFallback for OLX
      );

      geoId = geoResolution.geoId ?? undefined;
      streetId = geoResolution.streetId ?? undefined;

      if (geoResolution.streetMatchMethod && geoResolution.streetMatchMethod !== 'nearest') {
        this.logger.debug(
          `Street matched by ${geoResolution.streetMatchMethod} for aggregator property ${data.id}: streetId=${streetId}`,
        );
      }
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
      apartmentNumber: this.extractNumber(attrs.apartmentNumber) ?? undefined,
      corps: (attrs.corps as string) || undefined,
      lat: data.lat || undefined,
      lng: data.lng || undefined,
      price: price || undefined,
      currency: data.currency || 'USD',
      pricePerMeter: pricePerMeter ?? undefined,
      totalArea: totalArea ?? undefined,
      livingArea: livingArea ?? undefined,
      kitchenArea: kitchenArea ?? undefined,
      landArea: this.extractNumber(attrs.landArea) ?? undefined,
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

  private extractNumber(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return isNaN(value) ? null : value;
    if (typeof value === 'string') {
      const num = parseFloat(value);
      return isNaN(num) ? null : num;
    }
    return null;
  }
}
