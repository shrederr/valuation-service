import { Injectable } from '@nestjs/common';
import { SourceType, DealType, RealtyType, CONDITION_TYPE_MAP, PROJECT_TYPE_MAP } from '@libs/common';
import { UnifiedListing } from '@libs/database';
import { VectorPropertyEventDto } from '../dto';

@Injectable()
export class VectorPropertyMapper {
  /**
   * Maps CustomerProperty from vector-api to UnifiedListing
   */
  mapToUnifiedListing(data: VectorPropertyEventDto): Partial<UnifiedListing> {
    const dealType = this.mapDealType(data.dealType);
    const realtyType = this.mapRealtyType(data.realtyType);
    const price = this.extractPrice(data.attributes);
    // vector-api uses 'square_total' for total area
    const totalArea = this.extractNumber(data.attributes?.square_total);

    // Land area: vector stores in sotki (сотки), convert to m² (1 сотка = 100 м²)
    const landAreaSotki = this.extractNumber(data.attributes?.square_land_total);
    const landArea = landAreaSotki ? Math.round(landAreaSotki * 100 * 100) / 100 : null;

    // Price per meter: for area/land use landArea, otherwise totalArea
    const effectiveArea = (realtyType === RealtyType.Area && landArea) ? landArea : totalArea;
    const pricePerMeter = effectiveArea && price
      ? Math.round((price / effectiveArea) * 100) / 100
      : undefined;

    return {
      sourceType: SourceType.VECTOR,
      sourceId: data.id,
      sourceGlobalId: data.globalId,
      dealType,
      realtyType,
      realtySubtype: data.realtySubtype || undefined,
      geoId: data.geoId || undefined,
      streetId: data.streetId || undefined,
      topzoneId: data.topzoneId || undefined,
      complexId: data.complexId || undefined,
      houseNumber: data.houseNumber || undefined,
      apartmentNumber: data.apartmentNumber ? parseInt(data.apartmentNumber, 10) : undefined,
      corps: data.corps || undefined,
      lat: data.lat || undefined,
      lng: data.lng || undefined,
      price: price ?? undefined,
      currency: (data.attributes?.currency as string) || 'USD',
      pricePerMeter,
      totalArea: totalArea ?? undefined,
      // vector-api attribute keys mapping:
      livingArea: this.extractNumber(data.attributes?.square_living) ?? undefined,
      kitchenArea: this.extractNumber(data.attributes?.square_kitchen) ?? undefined,
      landArea: landArea ?? undefined,
      rooms: this.extractNumber(data.attributes?.rooms_count) ?? undefined,
      floor: this.extractNumber(data.attributes?.floor) ?? undefined,
      totalFloors: this.extractNumber(data.attributes?.floors_count) ?? undefined,
      condition: this.mapConditionType(data.attributes?.condition_type) || undefined,
      houseType: this.mapProjectType(data.attributes?.project) || undefined,
      planningType: (data.attributes?.layout_features as string) || undefined,
      heatingType: (data.attributes?.heating_type as string) || undefined,
      attributes: data.attributes || undefined,
      cadastralNumber: data.cadastralNumber ? { number: data.cadastralNumber } : undefined,
      isActive: true,
      isExclusive: Boolean(data.attributes?.isExclusive) || false,
      syncedAt: new Date(),
    };
  }

  private mapDealType(dealType: string): DealType {
    const normalized = dealType?.toLowerCase();
    if (normalized === 'buy' || normalized === 'sell') {
      return DealType.Sell;
    }
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

  private extractPrice(attributes: Record<string, unknown> | undefined): number | null {
    if (!attributes) return null;
    const price = (attributes.price || attributes.priceUsd || attributes.priceUah) as number | undefined;
    return typeof price === 'number' ? price : null;
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

  private mapConditionType(value: unknown): string | undefined {
    const id = this.extractInteger(value);
    if (id === undefined) return undefined;
    return CONDITION_TYPE_MAP[id];
  }

  private mapProjectType(value: unknown): string | undefined {
    const id = this.extractInteger(value);
    if (id === undefined) return undefined;
    return PROJECT_TYPE_MAP[id];
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
