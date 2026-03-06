import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { DedupCheckService } from './services/dedup-check.service';
import { ToCrmMapper } from './mappers/to-crm.mapper';
import { CrmClientService } from './services/crm-client.service';
import { ExportStatsDto } from './dto';

@Injectable()
export class ExportService {
  private readonly logger = new Logger(ExportService.name);
  private readonly batchSize: number;
  private readonly enabled: boolean;
  private running = false;
  private lastRunAt: Date | null = null;
  private lastRunStats: { exported: number; duplicates: number; errors: number; skipped: number } | null = null;

  constructor(
    private readonly dataSource: DataSource,
    private readonly configService: ConfigService,
    private readonly dedupCheckService: DedupCheckService,
    private readonly toCrmMapper: ToCrmMapper,
    private readonly crmClientService: CrmClientService,
  ) {
    this.batchSize = parseInt(this.configService.get('EXPORT_BATCH_SIZE', '100'), 10);
    this.enabled = this.configService.get('EXPORT_ENABLED', 'false') === 'true';
  }

  @Cron(process.env.EXPORT_CRON || '0 */30 * * *')
  async cronExport() {
    if (!this.enabled) return;
    await this.runExport();
  }

  async runExport(overrideBatchSize?: number): Promise<{ exported: number; duplicates: number; errors: number; skipped: number }> {
    if (this.running) {
      this.logger.warn('Export already running, skipping');
      return { exported: 0, duplicates: 0, errors: 0, skipped: 0 };
    }

    this.running = true;
    const stats = { exported: 0, duplicates: 0, errors: 0, skipped: 0 };
    const batch = overrideBatchSize || this.batchSize;

    try {
      this.logger.log(`Export run started (batch=${batch})`);

      // Process new objects
      const newStats = await this.processNewObjects(batch);
      stats.exported += newStats.exported;
      stats.duplicates += newStats.duplicates;
      stats.errors += newStats.errors;
      stats.skipped += newStats.skipped;

      // Process updated objects
      const updStats = await this.processUpdatedObjects(batch);
      stats.exported += updStats.exported;
      stats.errors += updStats.errors;

      this.logger.log(
        `Export run done: exported=${stats.exported}, duplicates=${stats.duplicates}, errors=${stats.errors}, skipped=${stats.skipped}`,
      );
    } finally {
      this.running = false;
      this.lastRunAt = new Date();
      this.lastRunStats = stats;
    }

    return stats;
  }

  private async processNewObjects(limit: number) {
    const stats = { exported: 0, duplicates: 0, errors: 0, skipped: 0 };

    const listings = await this.dataSource.query(
      `SELECT * FROM unified_listings
       WHERE source_type = 'aggregator'
         AND is_active = true
         AND export_status IS NULL
         AND deleted_at IS NULL
       ORDER BY updated_at ASC
       LIMIT $1`,
      [limit],
    );

    for (const raw of listings) {
      try {
        const listing = this.hydrate(raw);

        // Validate
        if (!listing.price || !listing.totalArea) {
          await this.updateExportStatus(listing.id, 'skipped', null, 'Missing price or area');
          stats.skipped++;
          continue;
        }

        // Dedup check
        const dedup = await this.dedupCheckService.isDuplicate(listing);
        if (dedup.isDuplicate) {
          await this.updateExportStatus(listing.id, 'duplicate', null,
            `Matched ${dedup.matchLevel}: ${dedup.matchedId}${dedup.similarity ? ` (sim=${dedup.similarity.toFixed(3)})` : ''}`);
          stats.duplicates++;
          continue;
        }

        // Map and send
        const dto = await this.toCrmMapper.map(listing);
        const result = await this.crmClientService.createObject(dto);
        await this.updateExportStatus(listing.id, 'exported', result.id);
        stats.exported++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.updateExportStatus(raw.id, 'error', null, msg);
        stats.errors++;
        this.logger.error(`Export error for ${raw.id}: ${msg}`);
      }
    }

    return stats;
  }

  private async processUpdatedObjects(limit: number) {
    const stats = { exported: 0, errors: 0 };

    const listings = await this.dataSource.query(
      `SELECT * FROM unified_listings
       WHERE source_type = 'aggregator'
         AND is_active = true
         AND export_status = 'exported'
         AND crm_external_id IS NOT NULL
         AND updated_at > last_exported_at
         AND deleted_at IS NULL
       ORDER BY updated_at ASC
       LIMIT $1`,
      [limit],
    );

    for (const raw of listings) {
      try {
        const listing = this.hydrate(raw);
        const dto = await this.toCrmMapper.map(listing);
        await this.crmClientService.updateObject(raw.crm_external_id, dto);
        await this.updateExportStatus(listing.id, 'exported', raw.crm_external_id);
        stats.exported++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.updateExportStatus(raw.id, 'error', raw.crm_external_id, msg);
        stats.errors++;
      }
    }

    return stats;
  }

  async previewExport(listingId: string) {
    const rows = await this.dataSource.query(
      `SELECT * FROM unified_listings WHERE id = $1`,
      [listingId],
    );
    if (rows.length === 0) return null;

    const listing = this.hydrate(rows[0]);
    const dedup = await this.dedupCheckService.isDuplicate(listing);
    const dto = await this.toCrmMapper.map(listing);

    return { listing: rows[0], dedup, exportDto: dto };
  }

  async getStats(): Promise<ExportStatsDto> {
    const result = await this.dataSource.query(`
      SELECT
        COUNT(*) FILTER (WHERE source_type = 'aggregator' AND is_active = true AND deleted_at IS NULL) as total,
        COUNT(*) FILTER (WHERE export_status = 'exported') as exported,
        COUNT(*) FILTER (WHERE export_status = 'duplicate') as duplicate,
        COUNT(*) FILTER (WHERE export_status = 'error') as error,
        COUNT(*) FILTER (WHERE export_status = 'skipped') as skipped,
        COUNT(*) FILTER (WHERE source_type = 'aggregator' AND is_active = true AND deleted_at IS NULL AND export_status IS NULL) as pending
      FROM unified_listings
      WHERE source_type = 'aggregator'
    `);

    const row = result[0];
    return {
      total: parseInt(row.total, 10),
      exported: parseInt(row.exported, 10),
      duplicate: parseInt(row.duplicate, 10),
      error: parseInt(row.error, 10),
      skipped: parseInt(row.skipped, 10),
      pending: parseInt(row.pending, 10),
    };
  }

  getStatus() {
    return {
      running: this.running,
      enabled: this.enabled,
      crmConfigured: this.crmClientService.isConfigured(),
      lastRunAt: this.lastRunAt,
      lastRunStats: this.lastRunStats,
    };
  }

  private async updateExportStatus(id: string, status: string, crmExternalId: string | null, error?: string) {
    await this.dataSource.query(
      `UPDATE unified_listings
       SET export_status = $1, crm_external_id = COALESCE($2, crm_external_id), last_exported_at = NOW(), export_error = $3
       WHERE id = $4`,
      [status, crmExternalId, error || null, id],
    );
  }

  /** Convert raw DB row to UnifiedListing-like object */
  private hydrate(raw: Record<string, unknown>): UnifiedListing {
    const listing = new UnifiedListing();
    Object.assign(listing, {
      id: raw.id,
      sourceType: raw.source_type,
      sourceId: raw.source_id,
      dealType: raw.deal_type,
      realtyType: raw.realty_type,
      geoId: raw.geo_id ? Number(raw.geo_id) : undefined,
      streetId: raw.street_id ? Number(raw.street_id) : undefined,
      topzoneId: raw.topzone_id ? Number(raw.topzone_id) : undefined,
      complexId: raw.complex_id ? Number(raw.complex_id) : undefined,
      houseNumber: raw.house_number,
      lat: raw.lat ? Number(raw.lat) : undefined,
      lng: raw.lng ? Number(raw.lng) : undefined,
      price: raw.price ? Number(raw.price) : undefined,
      currency: raw.currency,
      pricePerMeter: raw.price_per_meter ? Number(raw.price_per_meter) : undefined,
      totalArea: raw.total_area ? Number(raw.total_area) : undefined,
      livingArea: raw.living_area ? Number(raw.living_area) : undefined,
      kitchenArea: raw.kitchen_area ? Number(raw.kitchen_area) : undefined,
      landArea: raw.land_area ? Number(raw.land_area) : undefined,
      rooms: raw.rooms ? Number(raw.rooms) : undefined,
      floor: raw.floor ? Number(raw.floor) : undefined,
      totalFloors: raw.total_floors ? Number(raw.total_floors) : undefined,
      condition: raw.condition,
      houseType: raw.house_type,
      attributes: raw.attributes,
      description: raw.description,
      isActive: raw.is_active,
      realtyPlatform: raw.realty_platform,
      normalizedPhone: raw.normalized_phone,
      externalUrl: raw.external_url,
      primaryData: raw.primary_data,
      publishedAt: raw.published_at ? new Date(raw.published_at as string) : undefined,
      updatedAt: raw.updated_at ? new Date(raw.updated_at as string) : undefined,
      exportStatus: raw.export_status,
      crmExternalId: raw.crm_external_id,
    });
    return listing;
  }
}
