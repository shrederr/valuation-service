import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { GeoType } from '@libs/common';
import { GeoRepository, StreetRepository, Geo, Street } from '@libs/database';
import { StreetMatcherService, StreetMatchMethod } from './street-matcher.service';

export type GeoLookupResult = {
  geo: Geo | null;
  street: Street | null;
  allGeos: Geo[];
};

export type GeoResolutionResult = {
  geoId: number | null;
  streetId: number | null;
  isCityLevel: boolean;
  streetMatchMethod?: StreetMatchMethod;
};

@Injectable()
export class GeoLookupService {
  private readonly logger = new Logger(GeoLookupService.name);

  // Cache for resolveGeoForListing results (key: "lng,lat" rounded to 5 decimals = ~1m precision)
  private readonly resolveCache = new Map<string, { geoId: number | null; streetId: number | null; isCityLevel: boolean }>();
  private readonly CACHE_PRECISION = 5; // 5 decimals = ~1.1m precision

  public constructor(
    private readonly geoRepository: GeoRepository,
    private readonly streetRepository: StreetRepository,
    @Inject(forwardRef(() => StreetMatcherService))
    private readonly streetMatcherService: StreetMatcherService,
  ) {}

  private getCacheKey(lng: number, lat: number): string {
    return `${lng.toFixed(this.CACHE_PRECISION)},${lat.toFixed(this.CACHE_PRECISION)}`;
  }

  public async findGeoByCoordinates(lng: number, lat: number): Promise<Geo | null> {
    return this.geoRepository.findGeoByPoint(lng, lat);
  }

  public async findAllGeosByCoordinates(lng: number, lat: number): Promise<Geo[]> {
    return this.geoRepository.findAllGeosByPoint(lng, lat);
  }

  public async findStreetByCoordinates(lng: number, lat: number, geoId?: number): Promise<Street | null> {
    return this.streetRepository.findNearestStreet(lng, lat, geoId);
  }

  public async lookupByCoordinates(lng: number, lat: number): Promise<GeoLookupResult> {
    const allGeos = await this.geoRepository.findAllGeosByPoint(lng, lat);

    const geo = allGeos.length > 0 ? allGeos[0] : null;

    let street: Street | null = null;

    if (geo) {
      street = await this.streetRepository.findNearestStreet(lng, lat, geo.id);

      if (!street) {
        street = await this.streetRepository.findNearestStreet(lng, lat);
      }
    } else {
      street = await this.streetRepository.findNearestStreet(lng, lat);
    }

    return { geo, street, allGeos };
  }

  public async findCityByCoordinates(lng: number, lat: number): Promise<Geo | null> {
    const allGeos = await this.geoRepository.findAllGeosByPoint(lng, lat);

    return allGeos.find((g) => g.type === GeoType.City || g.type === GeoType.Village) || null;
  }

  public async findDistrictByCoordinates(lng: number, lat: number): Promise<Geo | null> {
    const allGeos = await this.geoRepository.findAllGeosByPoint(lng, lat);

    return allGeos.find((g) => g.type === GeoType.CityDistrict || g.type === GeoType.RegionDistrict) || null;
  }

  public async findRegionByCoordinates(lng: number, lat: number): Promise<Geo | null> {
    const allGeos = await this.geoRepository.findAllGeosByPoint(lng, lat);

    return allGeos.find((g) => g.type === GeoType.Region) || null;
  }

  public async getFullGeoHierarchy(lng: number, lat: number): Promise<{
    country: Geo | null;
    region: Geo | null;
    regionDistrict: Geo | null;
    city: Geo | null;
    cityDistrict: Geo | null;
    street: Street | null;
  }> {
    const allGeos = await this.geoRepository.findAllGeosByPoint(lng, lat);

    const hierarchy = {
      country: allGeos.find((g) => g.type === GeoType.Country) || null,
      region: allGeos.find((g) => g.type === GeoType.Region) || null,
      regionDistrict: allGeos.find((g) => g.type === GeoType.RegionDistrict) || null,
      city: allGeos.find((g) => g.type === GeoType.City || g.type === GeoType.Village) || null,
      cityDistrict: allGeos.find((g) => g.type === GeoType.CityDistrict) || null,
      street: null as Street | null,
    };

    const geoIdForStreet = hierarchy.cityDistrict?.id || hierarchy.city?.id;

    if (geoIdForStreet) {
      hierarchy.street = await this.streetRepository.findNearestStreet(lng, lat, geoIdForStreet);
    }

    if (!hierarchy.street) {
      hierarchy.street = await this.streetRepository.findNearestStreet(lng, lat);
    }

    return hierarchy;
  }

  // Allowed geo types for listing binding - NOT region or region_district
  private readonly ALLOWED_GEO_TYPES = [GeoType.City, GeoType.Village, GeoType.CityDistrict];

  public async resolveGeoForListing(
    lng: number | undefined,
    lat: number | undefined,
    existingGeoId?: number,
  ): Promise<{ geoId: number | null; streetId: number | null; isCityLevel: boolean }> {
    if (!lng || !lat) {
      return { geoId: existingGeoId || null, streetId: null, isCityLevel: false };
    }

    // Check cache first
    const cacheKey = this.getCacheKey(lng, lat);
    const cached = this.resolveCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    // First, try to find geo by point (point inside polygon) with allowed types only
    let geo = await this.geoRepository.findGeoByPointAndTypes(lng, lat, this.ALLOWED_GEO_TYPES);
    let isCityLevel = false;

    // If not found (e.g., small village polygon), find nearest geo of allowed types
    if (!geo) {
      geo = await this.geoRepository.findNearestGeoByTypes(lng, lat, this.ALLOWED_GEO_TYPES);
      if (geo) {
        this.logger.debug(`Geo ${geo.id} (${geo.name?.uk}) found as nearest for point [${lng}, ${lat}]`);
      }
    }

    // Check if it's city level (not district)
    if (geo && (geo.type === GeoType.City || geo.type === GeoType.Village)) {
      isCityLevel = true;
    }

    // Find street
    let street: Street | null = null;
    if (geo) {
      street = await this.streetRepository.findNearestStreet(lng, lat, geo.id);
    }
    if (!street) {
      street = await this.streetRepository.findNearestStreet(lng, lat);
    }

    const result = {
      geoId: geo?.id || existingGeoId || null,
      streetId: street?.id || null,
      isCityLevel,
    };

    // Cache the result
    this.resolveCache.set(cacheKey, result);

    return result;
  }

  // Clear cache (useful for batch processing cleanup)
  public clearCache(): void {
    this.resolveCache.clear();
  }

  /**
   * Resolve geo and street for listing using text-based matching
   * This method uses StreetMatcherService for improved street matching
   */
  public async resolveGeoForListingWithText(
    lng: number | undefined,
    lat: number | undefined,
    text?: string,
    existingGeoId?: number,
  ): Promise<GeoResolutionResult> {
    if (!lng || !lat) {
      return { geoId: existingGeoId || null, streetId: null, isCityLevel: false };
    }

    // First, try to find geo by point (point inside polygon) with allowed types only
    let geo = await this.geoRepository.findGeoByPointAndTypes(lng, lat, this.ALLOWED_GEO_TYPES);
    let isCityLevel = false;

    // If not found (e.g., small village polygon), find nearest geo of allowed types
    if (!geo) {
      geo = await this.geoRepository.findNearestGeoByTypes(lng, lat, this.ALLOWED_GEO_TYPES);
      if (geo) {
        this.logger.debug(`Geo ${geo.id} (${geo.name?.uk}) found as nearest for point [${lng}, ${lat}]`);
      }
    }

    // Check if it's city level (not district)
    if (geo && (geo.type === GeoType.City || geo.type === GeoType.Village)) {
      isCityLevel = true;
    }

    // Use StreetMatcherService for smart street matching
    const streetResult = await this.streetMatcherService.resolveStreet(lng, lat, text, geo?.id);

    return {
      geoId: geo?.id || existingGeoId || null,
      streetId: streetResult.streetId,
      isCityLevel,
      streetMatchMethod: streetResult.matchMethod,
    };
  }
}
