import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { PrimaryDataExtractor } from './primary-data-extractor';
import { PhotoDedupService, PhotoCompareResult } from './photo-dedup.service';

const PRICE_DELTA = 0.05;
const RELAXED_PRICE_DELTA = 0.15;
const RELAXED_AREA_DELTA = 0.10;
const RELAXED_GEO_RADIUS = 100; // meters
const PHOTO_CONFIDENCE_THRESHOLD = 0.7;

/**
 * Source filter for dedup: match against CRM objects AND already-exported aggregator objects.
 * This prevents cross-platform duplicates (e.g. same property on OLX already exported,
 * then appearing from DomRia).
 */
const DEDUP_SOURCE_FILTER = `(source_type IN ('vector', 'vector_crm') OR (source_type = 'aggregator' AND export_status = 'exported'))`;

/**
 * Activity filter: CRM objects are checked even if inactive/archived (an archived CRM listing
 * still represents the same physical property — re-exporting would create duplicates).
 * Aggregator objects must be active to be dedup targets.
 */
const DEDUP_ACTIVE_FILTER = `(is_active = true OR source_type IN ('vector', 'vector_crm'))`;

/**
 * Exact attribute keys that must match per realty type (from api_realty_comparison).
 * These are the DB columns we check for exact equality.
 */
const REALTY_TYPE_ATTR_KEYS: Record<string, string[]> = {
  apartment: ['total_floors', 'total_area', 'floor'],
  house: ['total_area', 'total_floors'],
  commercial: ['total_area', 'total_floors'],
  area: ['land_area'],
};

export interface DedupResult {
  isDuplicate: boolean;
  matchedId?: string;
  matchLevel?: 'address' | 'geo' | 'text' | 'photo';
  similarity?: number;
  photoVerdict?: 'SAME' | 'DIFFERENT' | 'UNCERTAIN' | 'ERROR';
  pendingPhotoCheck?: boolean;
}

@Injectable()
export class DedupCheckService {
  private readonly logger = new Logger(DedupCheckService.name);

  constructor(
    private readonly dataSource: DataSource,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
    private readonly photoDedupService: PhotoDedupService,
  ) {}

  async isDuplicate(listing: UnifiedListing): Promise<DedupResult> {
    // Level 1: Exact address match
    const addressMatch = await this.checkByAddress(listing);
    if (addressMatch) {
      return { isDuplicate: true, matchedId: addressMatch, matchLevel: 'address' };
    }

    // Level 2: Geo proximity 50m + exact attributes
    const geoMatch = await this.checkByGeoProximity(listing);
    if (geoMatch) {
      return { isDuplicate: true, matchedId: geoMatch, matchLevel: 'geo' };
    }

    // Level 3: Relaxed geo (100m) + photo confirmation via GPT-4o
    if (this.photoDedupService.isEnabled()) {
      const photoResult = await this.checkByPhotoDedup(listing);
      if (photoResult) return photoResult;
    }

    return { isDuplicate: false };
  }

  /**
   * Level 3: Relaxed geo proximity + GPT-4o Vision photo comparison.
   * Finds suspicious neighbors (100m, ±15% price, ±10% area),
   * then confirms by comparing photos.
   */
  private async checkByPhotoDedup(listing: UnifiedListing): Promise<DedupResult | null> {
    // Check if listing has photos — if not, mark as no_photos (don't export)
    const listingPhotos = this.extractPhotoUrls(listing);
    if (!listingPhotos || listingPhotos.length === 0) {
      return {
        isDuplicate: true,
        matchLevel: 'photo',
        photoVerdict: 'ERROR',
        pendingPhotoCheck: false,
      };
    }

    const candidates = await this.checkByGeoProximityRelaxed(listing);
    if (!candidates || candidates.length === 0) return null;

    for (const candidateId of candidates) {
      const result = await this.confirmWithPhotos(listing, candidateId);
      if (result.verdict === 'SAME' && result.confidence >= PHOTO_CONFIDENCE_THRESHOLD) {
        return {
          isDuplicate: true,
          matchedId: candidateId,
          matchLevel: 'photo',
          similarity: result.confidence,
          photoVerdict: 'SAME',
        };
      }
    }

    return null;
  }

  /** Extract photo URLs from listing attributes/primaryData */
  private extractPhotoUrls(listing: UnifiedListing): string[] | null {
    const data = this.primaryDataExtractor.extractForExport(listing);
    return data.photos?.filter(url =>
      typeof url === 'string' && url.startsWith('http') && url.length > 10,
    ) || null;
  }

  /** Level 1: Exact match by address + params + price (±10%) */
  private async checkByAddress(listing: UnifiedListing): Promise<string | null> {
    if (!listing.streetId || !listing.houseNumber) return null;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE ${DEDUP_SOURCE_FILTER}
         AND ${DEDUP_ACTIVE_FILTER}
         AND id != $1
         AND street_id = $2
         AND house_number = $3
         AND ($4::int IS NULL OR rooms = $4)
         AND ($5::numeric IS NULL OR ABS(total_area - $5) <= 2)
         AND ($6::numeric IS NULL OR ABS(price - $6) / NULLIF(GREATEST(price, $6), 0) <= 0.10)
       LIMIT 1`,
      [
        listing.id,
        listing.streetId,
        listing.houseNumber,
        listing.rooms ?? null,
        listing.totalArea ?? null,
        listing.price ?? null,
      ],
    );

    return result.length > 0 ? result[0].id : null;
  }

  /**
   * Level 3: Geo proximity (50m) + exact attribute match + price ±5% (PostGIS)
   *
   * Following api_realty_comparison approach: exact match on key attributes
   * per realty type (total_floors, total_area, floor for apartments) to avoid
   * false positives in large complexes where many apartments share similar params.
   */
  private async checkByGeoProximity(listing: UnifiedListing): Promise<string | null> {
    if (!listing.lat || !listing.lng) return null;

    const attrKeys = REALTY_TYPE_ATTR_KEYS[listing.realtyType] || [];
    const attrConditions = this.buildAttrConditions(listing, attrKeys, 8);

    // If we don't have enough attributes for meaningful matching, skip Level 3
    // (we need at least area to avoid false positives)
    if (!listing.totalArea && !listing.landArea) return null;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE ${DEDUP_SOURCE_FILTER}
         AND ${DEDUP_ACTIVE_FILTER}
         AND id != $1
         AND realty_type = $2
         AND lat IS NOT NULL AND lng IS NOT NULL
         AND ST_DWithin(
           geography(ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)),
           geography(ST_SetSRID(ST_MakePoint($3::float8, $4::float8), 4326)),
           50
         )
         AND ($5::int IS NULL OR rooms = $5)
         AND price > 0 AND $6::numeric > 0
         AND ABS(price - $6) / NULLIF(GREATEST(price, $6), 0) <= $7
         ${attrConditions.sql}
       LIMIT 1`,
      [
        listing.id,
        listing.realtyType,
        listing.lng,
        listing.lat,
        listing.rooms ?? null,
        listing.price ?? 0,
        PRICE_DELTA,
        ...attrConditions.params,
      ],
    );

    return result.length > 0 ? result[0].id : null;
  }

  /**
   * Level 3.5a: Relaxed geo proximity — wider thresholds to find suspicious candidates.
   * 100m radius, ±15% price, ±10% area, no exact floor/total_floors match.
   * Returns up to 3 candidate IDs for photo verification.
   */
  private async checkByGeoProximityRelaxed(listing: UnifiedListing): Promise<string[] | null> {
    if (!listing.lat || !listing.lng) return null;
    if (!listing.totalArea && !listing.landArea) return null;

    const areaColumn = listing.totalArea ? 'total_area' : 'land_area';
    const areaValue = listing.totalArea || listing.landArea;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE ${DEDUP_SOURCE_FILTER}
         AND ${DEDUP_ACTIVE_FILTER}
         AND id != $1
         AND realty_type = $2
         AND lat IS NOT NULL AND lng IS NOT NULL
         AND ST_DWithin(
           geography(ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)),
           geography(ST_SetSRID(ST_MakePoint($3::float8, $4::float8), 4326)),
           ${RELAXED_GEO_RADIUS}
         )
         AND ($5::int IS NULL OR rooms = $5)
         AND price > 0 AND $6::numeric > 0
         AND ABS(price - $6) / NULLIF(GREATEST(price, $6), 0) <= ${RELAXED_PRICE_DELTA}
         AND ${areaColumn} IS NOT NULL AND $7::numeric > 0
         AND ABS(${areaColumn} - $7) / NULLIF(GREATEST(${areaColumn}, $7), 0) <= ${RELAXED_AREA_DELTA}
       LIMIT 3`,
      [
        listing.id,
        listing.realtyType,
        listing.lng,
        listing.lat,
        listing.rooms ?? null,
        listing.price ?? 0,
        areaValue ?? 0,
      ],
    );

    if (result.length === 0) return null;
    return result.map((r: { id: string }) => r.id);
  }

  /**
   * Level 3.5b: Confirm a suspected duplicate by comparing photos via GPT-4o Vision.
   */
  private async confirmWithPhotos(
    listing: UnifiedListing,
    candidateId: string,
  ): Promise<PhotoCompareResult> {
    try {
      const candidate = await this.photoDedupService.loadListing(candidateId);
      if (!candidate) {
        return { verdict: 'ERROR', confidence: 0, reasoning: `Candidate ${candidateId} not found` };
      }
      return await this.photoDedupService.compare(listing, candidate);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.error(`Photo confirmation failed for ${candidateId}: ${msg}`);
      return { verdict: 'ERROR', confidence: 0, reasoning: msg };
    }
  }

  /**
   * Build SQL conditions for exact attribute matching per realty type.
   * Mirrors api_realty_comparison's `term` queries on attributes.
   *
   * @param paramOffset - starting parameter index (1-based, after existing params)
   */
  private buildAttrConditions(
    listing: UnifiedListing,
    attrKeys: string[],
    paramOffset = 6,
  ): { sql: string; params: unknown[] } {
    const clauses: string[] = [];
    const params: unknown[] = [];
    let idx = paramOffset;

    for (const key of attrKeys) {
      const value = this.getAttrValue(listing, key);
      if (value === null || value === undefined) continue;

      // Exact match for key attributes (like api_realty_comparison's term queries)
      clauses.push(`AND ${this.columnForAttrKey(key)} = $${idx}`);
      params.push(value);
      idx++;
    }

    return { sql: clauses.join('\n         '), params };
  }

  /** Map attribute key → listing property value */
  private getAttrValue(listing: UnifiedListing, key: string): number | null {
    switch (key) {
      case 'total_area':
      case 'square_total':
        return listing.totalArea ?? null;
      case 'floor':
        return listing.floor ?? null;
      case 'total_floors':
      case 'floors_count':
        return listing.totalFloors ?? null;
      case 'land_area':
      case 'square_land_total':
        return listing.landArea ?? null;
      default:
        return null;
    }
  }

  /** Map attribute key → SQL column name */
  private columnForAttrKey(key: string): string {
    const map: Record<string, string> = {
      total_area: 'total_area',
      square_total: 'total_area',
      floor: 'floor',
      total_floors: 'total_floors',
      floors_count: 'total_floors',
      land_area: 'land_area',
      square_land_total: 'land_area',
    };
    const col = map[key];
    if (!col) {
      throw new Error(`Unknown attribute key for dedup: ${key}`);
    }
    return col;
  }
}
