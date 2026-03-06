import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { Vector2ExportDto } from '../dto';
import { PrimaryDataExtractor } from '../services/primary-data-extractor';

@Injectable()
export class ToCrmMapper {
  private readonly logger = new Logger(ToCrmMapper.name);
  private geoCache = new Map<number, number | null>();
  private streetCache = new Map<number, number | null>();

  constructor(
    private readonly dataSource: DataSource,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
  ) {}

  async map(listing: UnifiedListing): Promise<Vector2ExportDto> {
    const extracted = this.primaryDataExtractor.extractForExport(listing);

    const dto: Vector2ExportDto = {
      source_id: listing.sourceId,
      source_platform: listing.realtyPlatform || 'unknown',
      type_estate: this.mapRealtyType(listing.realtyType),
      deal_type: listing.dealType,
      fk_geo_id: listing.geoId ? await this.resolveGeoId(listing.geoId) : undefined,
      geo_street: listing.streetId ? await this.resolveStreetId(listing.streetId) : undefined,
      house_number: listing.houseNumber || undefined,
      map_x: listing.lat || undefined,
      map_y: listing.lng || undefined,
      price: listing.price || 0,
      price_per_meter: listing.pricePerMeter || undefined,
      currency: listing.currency || 'USD',
      square_total: listing.totalArea || undefined,
      square_living: listing.livingArea || undefined,
      square_kitchen: listing.kitchenArea || undefined,
      square_land_total: listing.landArea ? Math.round((listing.landArea / 100) * 100) / 100 : undefined,
      rooms: listing.rooms || undefined,
      floor: listing.floor || undefined,
      total_floors: listing.totalFloors || undefined,
      condition: listing.condition || undefined,
      house_type: listing.houseType || undefined,
      description: extracted.description || undefined,
      phones: extracted.phones || undefined,
      photos: extracted.photos || undefined,
      url: extracted.url || undefined,
      is_active: listing.isActive,
      published_at: listing.publishedAt?.toISOString() || undefined,
      updated_at: listing.updatedAt?.toISOString() || new Date().toISOString(),
    };

    return dto;
  }

  private mapRealtyType(realtyType: string): number {
    const map: Record<string, number> = {
      apartment: 1,
      house: 2,
      commercial: 3,
      area: 4,
      garage: 5,
      room: 6,
    };
    return map[realtyType] || 1;
  }

  /** Resolve local geoId → vector2 source_id via source_id_mappings */
  private async resolveGeoId(localId: number): Promise<number | undefined> {
    if (this.geoCache.has(localId)) {
      return this.geoCache.get(localId) ?? undefined;
    }

    try {
      const result = await this.dataSource.query(
        `SELECT source_id FROM source_id_mappings WHERE local_id = $1 AND entity_type = 'geo' AND source = 'vector2' LIMIT 1`,
        [localId],
      );
      const sourceId = result.length > 0 ? parseInt(result[0].source_id, 10) : null;
      this.geoCache.set(localId, sourceId);
      return sourceId ?? undefined;
    } catch {
      return undefined;
    }
  }

  /** Resolve local streetId → vector2 source_id via source_id_mappings */
  private async resolveStreetId(localId: number): Promise<number | undefined> {
    if (this.streetCache.has(localId)) {
      return this.streetCache.get(localId) ?? undefined;
    }

    try {
      const result = await this.dataSource.query(
        `SELECT source_id FROM source_id_mappings WHERE local_id = $1 AND entity_type = 'street' AND source = 'vector2' LIMIT 1`,
        [localId],
      );
      const sourceId = result.length > 0 ? parseInt(result[0].source_id, 10) : null;
      this.streetCache.set(localId, sourceId);
      return sourceId ?? undefined;
    } catch {
      return undefined;
    }
  }
}
