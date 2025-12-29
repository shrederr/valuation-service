import { Injectable, Logger } from '@nestjs/common';
import { SourceType, DealType, RealtyType, AttributeMapperService } from '@libs/common';
import { UnifiedListing } from '@libs/database';
import { AggregatorPropertyEventDto } from '../dto';
import { GeoLookupService, GeoResolutionResult } from '../../osm/geo-lookup.service';

export interface MappingResult {
  listing: Partial<UnifiedListing>;
  geoResolution: GeoResolutionResult | null;
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
  ) {}

  /**
   * Maps ExportedProperty from api-property-aggregator to UnifiedListing
   * Now async - uses GeoLookupService for geo/street resolution
   * and AttributeMapperService for condition/houseType mapping
   */
  async mapToUnifiedListing(data: AggregatorPropertyEventDto): Promise<MappingResult> {
    const dealType = this.mapDealType(data.dealType);
    const realtyType = this.mapRealtyType(data.realtyType);
    const totalArea = this.extractNumber(data.attributes?.totalArea);
    const price = data.price;

    // Detect platform from realty_platform or URL
    const platform = this.attributeMapperService.detectPlatform(
      data.realtyPlatform,
      data.url,
    );

    // Resolve geo and street by coordinates + text
    let geoResolution: GeoResolutionResult | null = null;
    let geoId = data.geoId || undefined;
    let streetId = data.streetId || undefined;

    if (data.lng && data.lat) {
      // Build text for street matching from address/title
      const textForMatching = this.buildTextForMatching(data);

      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        data.lng,
        data.lat,
        textForMatching,
        data.geoId,
      );

      if (geoResolution.geoId) {
        geoId = geoResolution.geoId;
      }
      if (geoResolution.streetId) {
        streetId = geoResolution.streetId;
      }

      // Log if we matched street by text (not nearest)
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
      topzoneId: data.topzoneId || undefined,
      complexId: data.complexId || undefined,
      houseNumber: data.houseNumber || undefined,
      apartmentNumber: this.extractNumber(data.attributes?.apartmentNumber) ?? undefined,
      corps: (data.attributes?.corps as string) || undefined,
      lat: data.lat || undefined,
      lng: data.lng || undefined,
      price: price || undefined,
      currency: data.currency || 'USD',
      pricePerMeter: totalArea && price ? price / totalArea : undefined,
      totalArea: totalArea ?? undefined,
      livingArea: this.extractNumber(data.attributes?.livingArea) ?? undefined,
      kitchenArea: this.extractNumber(data.attributes?.kitchenArea) ?? undefined,
      landArea: this.extractNumber(data.attributes?.landArea) ?? undefined,
      rooms: this.extractNumber(data.attributes?.rooms) ?? undefined,
      floor: this.extractNumber(data.attributes?.floor) ?? undefined,
      totalFloors: this.extractNumber(data.attributes?.totalFloors) ?? undefined,
      // Use mapped condition/houseType instead of raw values
      condition: attributeResult.condition || undefined,
      houseType: attributeResult.houseType || undefined,
      planningType: (data.attributes?.planningType as string) || undefined,
      heatingType: (data.attributes?.heatingType as string) || undefined,
      attributes: data.attributes || undefined,
      description: data.description ? { uk: data.description.uk || '' } : undefined,
      isActive: data.isActive ?? true,
      isExclusive: false,
      externalUrl: data.url || undefined,
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
      attributeFallbacks: {
        conditionFallback: attributeResult.conditionFallback ?? false,
        houseTypeFallback: attributeResult.houseTypeFallback ?? false,
      },
    };
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
