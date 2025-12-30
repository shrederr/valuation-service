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

    // Resolve geo and street by coordinates + text
    let geoResolution: GeoResolutionResult | null = null;
    let geoId = data.geoId || undefined;
    let streetId = data.streetId || undefined;

    // Extract coordinates - handle Prisma Decimal objects from aggregator
    // First try main payload, then fallback to primaryData
    let lng = this.extractNumber(data.lng);
    let lat = this.extractNumber(data.lat);

    // Fallback: try to extract from primaryData if main coords are missing
    if ((!lng || !lat) && data.primaryData) {
      const fallbackCoords = this.extractCoordsFromPrimaryData(data.primaryData, data.realtyPlatform);
      if (fallbackCoords.lat && fallbackCoords.lng) {
        lat = fallbackCoords.lat;
        lng = fallbackCoords.lng;
        this.logger.debug(`Property ${data.id} using coords from primaryData: lng=${lng}, lat=${lat}`);
      }
    }

    this.logger.debug(`Property ${data.id} raw coords: lng=${data.lng} (type: ${typeof data.lng}), lat=${data.lat} (type: ${typeof data.lat})`);
    this.logger.debug(`Property ${data.id} final coords: lng=${lng}, lat=${lat}`);

    if (lng && lat) {
      // Build text for street matching from address/title
      const textForMatching = this.buildTextForMatching(data);

      this.logger.debug(`Calling geoLookupService for property ${data.id}: lng=${lng}, lat=${lat}`);

      geoResolution = await this.geoLookupService.resolveGeoForListingWithText(
        lng,
        lat,
        textForMatching,
        data.geoId,
      );

      this.logger.debug(`GeoLookup result for property ${data.id}: geoId=${geoResolution.geoId}, streetId=${geoResolution.streetId}`);

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
    } else {
      this.logger.warn(`No coordinates for property ${data.id}: extracted lng=${lng}, lat=${lat} (raw: ${JSON.stringify(data.lng)}, ${JSON.stringify(data.lat)})`);
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
      apartmentNumber: this.extractNumber(attrs.apartmentNumber) ?? undefined,
      corps: (attrs.corps as string) || undefined,
      lat: lat ?? undefined,
      lng: lng ?? undefined,
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
    // Handle Prisma Decimal objects (they have toNumber() method or can be converted via toString)
    if (typeof value === 'object' && value !== null) {
      // Prisma Decimal has toNumber() method
      if ('toNumber' in value && typeof (value as any).toNumber === 'function') {
        return (value as any).toNumber();
      }
      // Or try toString() and parse
      if ('toString' in value) {
        const num = parseFloat(value.toString());
        return isNaN(num) ? null : num;
      }
    }
    return null;
  }

  /**
   * Try to extract coordinates from primaryData based on platform
   * This is a fallback when main lat/lng fields are missing from the event
   */
  private extractCoordsFromPrimaryData(
    primaryData: Record<string, unknown>,
    platform?: string,
  ): { lat: number | null; lng: number | null } {
    // DomRia: latitude, longitude fields directly
    if (primaryData.latitude !== undefined && primaryData.longitude !== undefined) {
      const lat = this.extractNumber(primaryData.latitude);
      const lng = this.extractNumber(primaryData.longitude);
      if (lat && lng) return { lat, lng };
    }

    // OLX: map.lat, map.lon
    if (primaryData.map && typeof primaryData.map === 'object') {
      const map = primaryData.map as Record<string, unknown>;
      const lat = this.extractNumber(map.lat);
      const lng = this.extractNumber(map.lon);
      if (lat && lng) return { lat, lng };
    }

    // RealtorUa / MLS: description.location as "lng,lat" string
    if (primaryData.description && typeof primaryData.description === 'object') {
      const desc = primaryData.description as Record<string, unknown>;
      if (typeof desc.location === 'string' && desc.location.includes(',')) {
        const parts = desc.location.split(',');
        if (parts.length >= 2) {
          // RealtorUa format: "lng,lat"
          const lng = this.extractNumber(parts[0].trim());
          const lat = this.extractNumber(parts[1].trim());
          if (lat && lng) return { lat, lng };
        }
      }
    }

    // MLS: location field directly as "lat,lng" string
    if (typeof primaryData.location === 'string' && primaryData.location.includes(',')) {
      const parts = primaryData.location.split(',');
      if (parts.length >= 2) {
        // MLS format: "lat,lng"
        const lat = this.extractNumber(parts[0].trim());
        const lng = this.extractNumber(parts[1].trim());
        if (lat && lng) return { lat, lng };
      }
    }

    // RealEstateLviv: ad_lat, ad_long
    if (primaryData.ad_lat !== undefined && primaryData.ad_long !== undefined) {
      const lat = this.extractNumber(primaryData.ad_lat);
      const lng = this.extractNumber(primaryData.ad_long);
      if (lat && lng) return { lat, lng };
    }

    return { lat: null, lng: null };
  }
}
