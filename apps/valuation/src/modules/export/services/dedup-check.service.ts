import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { PrimaryDataExtractor } from './primary-data-extractor';
import { EmbeddingService } from './embedding.service';

export interface DedupResult {
  isDuplicate: boolean;
  matchedId?: string;
  matchLevel?: 'address' | 'phone' | 'geo' | 'text';
  similarity?: number;
}

@Injectable()
export class DedupCheckService {
  private readonly logger = new Logger(DedupCheckService.name);

  constructor(
    private readonly dataSource: DataSource,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
    private readonly embeddingService: EmbeddingService,
  ) {}

  async isDuplicate(listing: UnifiedListing): Promise<DedupResult> {
    const addressMatch = await this.checkByAddress(listing);
    if (addressMatch) {
      return { isDuplicate: true, matchedId: addressMatch, matchLevel: 'address' };
    }

    const phoneMatch = await this.checkByPhone(listing);
    if (phoneMatch) {
      return { isDuplicate: true, matchedId: phoneMatch, matchLevel: 'phone' };
    }

    const geoMatch = await this.checkByGeoProximity(listing);
    if (geoMatch) {
      return { isDuplicate: true, matchedId: geoMatch, matchLevel: 'geo' };
    }

    const textMatch = await this.checkByEmbeddingSimilarity(listing);
    if (textMatch) {
      return { isDuplicate: true, matchedId: textMatch.id, matchLevel: 'text', similarity: textMatch.similarity };
    }

    return { isDuplicate: false };
  }

  /** Level 1: Exact match by address + params + price (±10%) */
  private async checkByAddress(listing: UnifiedListing): Promise<string | null> {
    if (!listing.streetId || !listing.houseNumber) return null;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE source_type IN ('vector', 'vector_crm')
         AND is_active = true
         AND street_id = $1
         AND house_number = $2
         AND ($3::int IS NULL OR rooms = $3)
         AND ($4::numeric IS NULL OR ABS(total_area - $4) <= 2)
         AND ($5::numeric IS NULL OR ABS(price - $5) / NULLIF(price, 0) <= 0.10)
       LIMIT 1`,
      [
        listing.streetId,
        listing.houseNumber,
        listing.rooms ?? null,
        listing.totalArea ?? null,
        listing.price ?? null,
      ],
    );

    return result.length > 0 ? result[0].id : null;
  }

  /** Level 2: Match by normalized phone number */
  private async checkByPhone(listing: UnifiedListing): Promise<string | null> {
    const phone = listing.normalizedPhone || this.primaryDataExtractor.extractNormalizedPhone(listing);
    if (!phone) return null;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE source_type IN ('vector', 'vector_crm')
         AND is_active = true
         AND normalized_phone = $1
         AND realty_type = $2
       LIMIT 1`,
      [phone, listing.realtyType],
    );

    return result.length > 0 ? result[0].id : null;
  }

  /** Level 3: Geo proximity (50m) + area + rooms (PostGIS) */
  private async checkByGeoProximity(listing: UnifiedListing): Promise<string | null> {
    if (!listing.lat || !listing.lng) return null;

    const result = await this.dataSource.query(
      `SELECT id FROM unified_listings
       WHERE source_type IN ('vector', 'vector_crm')
         AND is_active = true
         AND realty_type = $1
         AND lat IS NOT NULL AND lng IS NOT NULL
         AND ST_DWithin(
           geography(ST_SetSRID(ST_MakePoint(lng::float8, lat::float8), 4326)),
           geography(ST_SetSRID(ST_MakePoint($2::float8, $3::float8), 4326)),
           50
         )
         AND ($4::numeric IS NULL OR ABS(total_area - $4) <= 2)
         AND ($5::int IS NULL OR rooms = $5)
       LIMIT 1`,
      [
        listing.realtyType,
        listing.lng,
        listing.lat,
        listing.totalArea ?? null,
        listing.rooms ?? null,
      ],
    );

    return result.length > 0 ? result[0].id : null;
  }

  /**
   * Level 4: Semantic embedding similarity (pgvector + HNSW)
   * Cosine similarity >= 0.92 → duplicate
   */
  private async checkByEmbeddingSimilarity(
    listing: UnifiedListing,
  ): Promise<{ id: string; similarity: number } | null> {
    if (!this.embeddingService.isReady()) return null;

    const description = this.primaryDataExtractor.extractForExport(listing).description;
    if (!description || description.length < 30) return null;
    if (!listing.geoId) return null;

    try {
      const queryEmbedding = await this.embeddingService.embed(description);
      const vectorStr = `[${queryEmbedding.join(',')}]`;

      const result = await this.dataSource.query(
        `SELECT id, 1 - (embedding <=> $1::vector) AS similarity
         FROM unified_listings
         WHERE source_type IN ('vector', 'vector_crm')
           AND is_active = true
           AND realty_type = $2
           AND geo_id = $3
           AND embedding IS NOT NULL
           AND ($4::numeric IS NULL OR ABS(total_area - $4) <= 10)
         ORDER BY embedding <=> $1::vector
         LIMIT 1`,
        [vectorStr, listing.realtyType, listing.geoId, listing.totalArea ?? null],
      );

      if (result.length > 0) {
        const similarity = parseFloat(result[0].similarity);
        if (similarity >= 0.92) {
          return { id: result[0].id, similarity };
        }
      }
    } catch (error) {
      this.logger.warn(
        `Embedding dedup failed: ${error instanceof Error ? error.message : error}`,
      );
    }

    return null;
  }
}
