import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { DedupCheckService } from './services/dedup-check.service';
import { TranslationService } from './services/translation.service';
import { ToCrmMapper } from './mappers/to-crm.mapper';
import { CrmClientService } from './services/crm-client.service';
import { StreetMatcherService } from '../osm/street-matcher.service';
import { ExportStatsDto } from './dto';

@Injectable()
export class ExportService {
  private readonly logger = new Logger(ExportService.name);
  private readonly batchSize: number;
  private readonly concurrency: number;
  private readonly throttleMs: number;
  private readonly enabled: boolean;
  private running = false;
  private runStartedAt: Date | null = null;
  private readonly LOCK_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
  private lastRunAt: Date | null = null;
  private lastRunStats: { exported: number; duplicates: number; errors: number; skipped: number } | null = null;

  constructor(
    private readonly dataSource: DataSource,
    private readonly configService: ConfigService,
    private readonly dedupCheckService: DedupCheckService,
    private readonly translationService: TranslationService,
    private readonly streetMatcherService: StreetMatcherService,
    private readonly toCrmMapper: ToCrmMapper,
    private readonly crmClientService: CrmClientService,
  ) {
    this.batchSize = parseInt(this.configService.get('EXPORT_BATCH_SIZE', '500'), 10);
    this.concurrency = parseInt(this.configService.get('EXPORT_CONCURRENCY', '5'), 10);
    this.throttleMs = parseInt(this.configService.get('EXPORT_THROTTLE_MS', '50'), 10);
    this.enabled = this.configService.get('EXPORT_ENABLED', 'false') === 'true';
  }

  @Cron(process.env.EXPORT_CRON || '0 */30 * * *')
  async cronExport() {
    if (!this.enabled) return;
    await this.runExport();
  }

  /**
   * Cron: archive exported objects that are no longer active on source platforms.
   * Runs every hour. Sends deleted_at to CRM via /import-object → handleArchive().
   */
  @Cron('0 15 * * * *')
  async cronDeactivate() {
    if (!this.enabled) return;
    await this.deactivateDeletedObjects();
  }

  async deactivateDeletedObjects(): Promise<{ archived: number; errors: number }> {
    const ids = await this.dataSource.query(`
      SELECT id FROM unified_listings
      WHERE export_status = 'exported'
        AND crm_external_id IS NOT NULL
        AND (deleted_at IS NOT NULL OR is_active = false)
      LIMIT 200
    `);

    if (!ids.length) {
      return { archived: 0, errors: 0 };
    }

    this.logger.log(`Deactivation: found ${ids.length} exported objects no longer active`);
    let archived = 0;
    let errors = 0;

    for (const { id } of ids) {
      try {
        // Load full listing and map to CRM DTO (CRM requires geo_id, type_estate, etc.)
        const rows = await this.dataSource.query(
          `SELECT * FROM unified_listings WHERE id = $1`, [id],
        );
        if (!rows.length) {
          this.logger.warn(`Deactivation: listing ${id} not found`);
          errors++;
          continue;
        }
        const listing = this.hydrate(rows[0]);

        const dto = await this.toCrmMapper.map(listing);
        if (!dto) {
          // Can't map → just mark as deactivated locally
          await this.dataSource.query(
            `UPDATE unified_listings SET export_status = 'deactivated' WHERE id = $1`,
            [id],
          );
          archived++;
          continue;
        }

        // Add deleted_at → CRM will call handleArchive()
        (dto as any).deleted_at = new Date().toISOString();

        const result = await this.crmClientService.importObject(dto);
        if (result.success) {
          await this.dataSource.query(
            `UPDATE unified_listings SET export_status = 'deactivated' WHERE id = $1`,
            [id],
          );
          archived++;
          this.logger.debug(`Deactivated: sourceId=${listing.sourceId}, crmId=${listing.crmExternalId}`);
        } else {
          this.logger.warn(`Deactivation failed: sourceId=${listing.sourceId}: ${result.error}`);
          errors++;
        }
      } catch (err) {
        this.logger.error(`Deactivation error: id=${id}: ${err}`);
        errors++;
      }
    }

    this.logger.log(`Deactivation complete: ${archived} archived, ${errors} errors`);
    return { archived, errors };
  }

  async runExport(opts?: { batchSize?: number; geoId?: number; realtyType?: string }): Promise<{
    exported: number; duplicates: number; errors: number; skipped: number;
    crmIds: { sourceId: number; crmId: string }[];
  }> {
    if (!this.acquireLock()) {
      this.logger.warn('Export already running, skipping');
      return { exported: 0, duplicates: 0, errors: 0, skipped: 0, crmIds: [] };
    }

    const stats = { exported: 0, duplicates: 0, errors: 0, skipped: 0, crmIds: [] as { sourceId: number; crmId: string }[] };
    const batch = opts?.batchSize || this.batchSize;
    const filters = { geoId: opts?.geoId, realtyType: opts?.realtyType };

    try {
      this.logger.log(`Export run started (batch=${batch}, geoId=${filters.geoId || 'all'}, realtyType=${filters.realtyType || 'all'})`);

      // Process new objects
      const newStats = await this.processNewObjects(batch, filters);
      stats.exported += newStats.exported;
      stats.duplicates += newStats.duplicates;
      stats.errors += newStats.errors;
      stats.skipped += newStats.skipped;
      stats.crmIds.push(...newStats.crmIds);

      // Re-export updated objects (price changes, etc.)
      const updStats = await this.processUpdatedObjects(Math.min(batch, 100));
      stats.exported += updStats.exported;
      stats.errors += updStats.errors;

      this.logger.log(
        `Export run done: new=${newStats.exported}, updated=${updStats.exported}, duplicates=${stats.duplicates}, errors=${stats.errors}, skipped=${stats.skipped}`,
      );
    } finally {
      this.releaseLock();
      this.lastRunAt = new Date();
      this.lastRunStats = { exported: stats.exported, duplicates: stats.duplicates, errors: stats.errors, skipped: stats.skipped };
    }

    return stats;
  }

  private async processNewObjects(limit: number, filters?: { geoId?: number; realtyType?: string }) {
    const stats = { exported: 0, duplicates: 0, errors: 0, skipped: 0, crmIds: [] as { sourceId: number; crmId: string }[] };

    // Build dynamic WHERE clause
    const conditions = [
      `source_type = 'aggregator'`,
      `is_active = true`,
      `export_status IS NULL`,
      `deleted_at IS NULL`,
    ];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (filters?.geoId) {
      conditions.push(`geo_id = $${paramIdx}`);
      params.push(filters.geoId);
      paramIdx++;
    }
    if (filters?.realtyType) {
      conditions.push(`realty_type = $${paramIdx}`);
      params.push(filters.realtyType);
      paramIdx++;
    }

    conditions.push(`price > 0 AND total_area > 0 AND geo_id IS NOT NULL`);

    params.push(limit);
    const limitParam = `$${paramIdx}`;

    const listings = await this.dataSource.query(
      `SELECT * FROM unified_listings
       WHERE ${conditions.join(' AND ')}
       ORDER BY updated_at ASC
       LIMIT ${limitParam}`,
      params,
    );

    // Process listings with concurrency pool
    await this.processWithConcurrency(listings, async (raw) => {
      try {
        const listing = this.hydrate(raw);

        // Dedup check
        const dedup = await this.dedupCheckService.isDuplicate(listing);
        if (dedup.isDuplicate) {
          await this.updateExportStatus(listing.id, 'duplicate', null,
            `Matched ${dedup.matchLevel}: ${dedup.matchedId}${dedup.similarity ? ` (sim=${dedup.similarity.toFixed(3)})` : ''}`);
          stats.duplicates++;
          return;
        }

        // Translate description if needed (UK↔RU)
        await this.translateIfNeeded(listing);

        // OLX: re-resolve street from text (coordinates unreliable)
        await this.fixOlxStreet(listing);

        // Map and send
        const dto = await this.toCrmMapper.map(listing);

        // Skip apartments without square_living (can't appear on CRM site).
        // Commercial and houses don't require living area.
        if (listing.realtyType === 'apartment' && !dto.attributes?.square_living) {
          await this.updateExportStatus(listing.id, 'skipped', null, 'Missing square_living: no living_area, no kitchen_area to calculate');
          stats.skipped++;
          return;
        }

        const result = await this.crmClientService.importObject(dto);
        if (!result.success) {
          await this.updateExportStatus(listing.id, 'error', null, result.error);
          stats.errors++;
          return;
        }
        await this.updateExportStatus(listing.id, 'exported', result.id || null);
        stats.exported++;
        stats.crmIds.push({ sourceId: listing.sourceId, crmId: result.id! });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.updateExportStatus(raw.id, 'error', null, msg);
        stats.errors++;
        this.logger.error(`Export error for ${raw.id}: ${msg}`);
      }
    });

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

    await this.processWithConcurrency(listings, async (raw) => {
      try {
        const listing = this.hydrate(raw);
        await this.translateIfNeeded(listing);
        const dto = await this.toCrmMapper.map(listing);
        const result = await this.crmClientService.importObject(dto);
        if (!result.success) {
          await this.updateExportStatus(listing.id, 'error', raw.crm_external_id, result.error);
          stats.errors++;
          return;
        }
        await this.updateExportStatus(listing.id, 'exported', result.id || raw.crm_external_id);
        stats.exported++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.updateExportStatus(raw.id, 'error', raw.crm_external_id, msg);
        stats.errors++;
      }
    });

    return stats;
  }

  async exportSingle(listingId: string) {
    const rows = await this.dataSource.query(
      `SELECT * FROM unified_listings WHERE id = $1`,
      [listingId],
    );
    if (rows.length === 0) return { error: 'Listing not found' };

    const listing = this.hydrate(rows[0]);

    // Validate
    if (!listing.price || !listing.totalArea) {
      return { error: 'Missing price or area', listing: rows[0] };
    }

    // Dedup check
    const dedup = await this.dedupCheckService.isDuplicate(listing);
    if (dedup.isDuplicate) {
      return { error: 'Listing is a duplicate', dedup };
    }

    // Translate description if needed
    await this.translateIfNeeded(listing);

    // OLX: re-resolve street from text (coordinates unreliable)
    await this.fixOlxStreet(listing);

    // Map
    const dto = await this.toCrmMapper.map(listing);

    // Check square_living
    if (!dto.attributes?.square_living) {
      return { error: 'Missing square_living: no living_area, no kitchen_area to calculate', exportDto: dto };
    }

    // Send to CRM
    const result = await this.crmClientService.importObject(dto);
    if (!result.success) {
      await this.updateExportStatus(listing.id, 'error', null, result.error);
      return { error: 'CRM import failed', details: result.error, exportDto: dto };
    }

    await this.updateExportStatus(listing.id, 'exported', result.id || null);
    return { status: 'exported', crmId: result.id, exportDto: dto };
  }

  async exportByPlatforms(opts: {
    platforms: string[];
    perPlatform: number;
    realtyType?: string;
  }): Promise<{
    total: { exported: number; duplicates: number; errors: number; skipped: number };
    byPlatform: Record<string, { exported: number; duplicates: number; errors: number; skipped: number; crmIds: { sourceId: number; crmId: string }[] }>;
  }> {
    if (!this.acquireLock()) {
      throw new Error('Export already running');
    }

    const total = { exported: 0, duplicates: 0, errors: 0, skipped: 0 };
    const byPlatform: Record<string, { exported: number; duplicates: number; errors: number; skipped: number; crmIds: { sourceId: number; crmId: string }[] }> = {};

    try {
      this.logger.log(`Export by platforms started: ${opts.platforms.join(', ')}, ${opts.perPlatform} per platform`);

      for (const platform of opts.platforms) {
        const platformStats = { exported: 0, duplicates: 0, errors: 0, skipped: 0, crmIds: [] as { sourceId: number; crmId: string }[] };
        byPlatform[platform] = platformStats;

        const conditions = [
          `source_type = 'aggregator'`,
          `is_active = true`,
          `export_status IS NULL`,
          `deleted_at IS NULL`,
          `price > 0`,
          `total_area > 0`,
          `geo_id IS NOT NULL`,
          `realty_platform = $1`,
        ];
        const params: unknown[] = [platform];
        let paramIdx = 2;

        if (opts.realtyType) {
          conditions.push(`realty_type = $${paramIdx}`);
          params.push(opts.realtyType);
          paramIdx++;
        }

        params.push(opts.perPlatform);

        const listings = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE ${conditions.join(' AND ')}
           ORDER BY published_at DESC NULLS LAST
           LIMIT $${paramIdx}`,
          params,
        );

        this.logger.log(`Platform ${platform}: ${listings.length} listings found`);

        await this.processWithConcurrency(listings, async (raw) => {
          try {
            const listing = this.hydrate(raw);

            const dedup = await this.dedupCheckService.isDuplicate(listing);
            if (dedup.isDuplicate) {
              await this.updateExportStatus(listing.id, 'duplicate', null,
                `Matched ${dedup.matchLevel}: ${dedup.matchedId}${dedup.similarity ? ` (sim=${dedup.similarity.toFixed(3)})` : ''}`);
              platformStats.duplicates++;
              total.duplicates++;
              return;
            }

            await this.translateIfNeeded(listing);

            // OLX: re-resolve street from text (coordinates unreliable)
            await this.fixOlxStreet(listing);

            const dto = await this.toCrmMapper.map(listing);

            // Skip apartments without square_living (can't appear on CRM site).
            // Commercial and houses don't require living area.
            if (listing.realtyType === 'apartment' && !dto.attributes?.square_living) {
              await this.updateExportStatus(listing.id, 'skipped', null, 'Missing square_living: no living_area, no kitchen_area to calculate');
              platformStats.skipped++;
              total.skipped++;
              return;
            }

            const result = await this.crmClientService.importObject(dto);
            if (!result.success) {
              await this.updateExportStatus(listing.id, 'error', null, result.error);
              platformStats.errors++;
              total.errors++;
              return;
            }

            await this.updateExportStatus(listing.id, 'exported', result.id || null);
            platformStats.exported++;
            total.exported++;
            platformStats.crmIds.push({ sourceId: listing.sourceId, crmId: result.id! });
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            await this.updateExportStatus(raw.id, 'error', null, msg);
            platformStats.errors++;
            total.errors++;
            this.logger.error(`Export error for ${raw.id} (${platform}): ${msg}`);
          }
        });

        this.logger.log(`Platform ${platform} done: exported=${platformStats.exported}, duplicates=${platformStats.duplicates}, errors=${platformStats.errors}`);
      }
    } finally {
      this.releaseLock();
      this.lastRunStats = total;
    }

    return { total, byPlatform };
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
      runStartedAt: this.runStartedAt,
    };
  }

  /** Acquire the export lock. Returns true if lock acquired, false if already running. Auto-releases stale locks after LOCK_TIMEOUT_MS. */
  private acquireLock(): boolean {
    if (this.running && this.runStartedAt) {
      const elapsed = Date.now() - this.runStartedAt.getTime();
      if (elapsed > this.LOCK_TIMEOUT_MS) {
        this.logger.warn(`Export lock timed out after ${Math.round(elapsed / 60000)}min — force releasing`);
        this.running = false;
        this.runStartedAt = null;
      }
    }
    if (this.running) return false;
    this.running = true;
    this.runStartedAt = new Date();
    return true;
  }

  /** Release the export lock. */
  private releaseLock(): void {
    this.running = false;
    this.runStartedAt = null;
    this.lastRunAt = new Date();
  }

  private async updateExportStatus(id: string, status: string, crmExternalId: string | null, error?: string) {
    await this.dataSource.query(
      `UPDATE unified_listings
       SET export_status = $1, crm_external_id = COALESCE($2, crm_external_id), last_exported_at = NOW(), export_error = $3
       WHERE id = $4`,
      [status, crmExternalId, error || null, id],
    );
  }

  /**
   * Translate descriptions for a batch of listings that are missing UK or RU.
   * Used for pre-translating before export.
   */
  async translateBatch(batchSize = 100): Promise<{ translated: number; errors: number; total: number }> {
    const stats = { translated: 0, errors: 0, total: 0 };

    if (!this.translationService.isEnabled()) {
      return { ...stats, total: -1 };
    }

    const listings = await this.dataSource.query(
      `SELECT * FROM unified_listings
       WHERE source_type = 'aggregator'
         AND is_active = true
         AND deleted_at IS NULL
         AND description IS NOT NULL
         AND (
           (description->>'uk' IS NOT NULL AND description->>'uk' != '' AND (description->>'ru' IS NULL OR description->>'ru' = ''))
           OR
           (description->>'ru' IS NOT NULL AND description->>'ru' != '' AND (description->>'uk' IS NULL OR description->>'uk' = ''))
         )
       ORDER BY updated_at DESC
       LIMIT $1`,
      [batchSize],
    );

    stats.total = listings.length;
    this.logger.log(`Translation batch: ${listings.length} listings to translate`);

    for (const raw of listings) {
      try {
        const listing = this.hydrate(raw);
        const updated = await this.translateIfNeeded(listing);
        if (updated) stats.translated++;
      } catch (err) {
        stats.errors++;
        this.logger.warn(`Translation error for ${raw.id}: ${err instanceof Error ? err.message : err}`);
      }
    }

    this.logger.log(`Translation batch done: ${stats.translated} translated, ${stats.errors} errors`);
    return stats;
  }

  /**
   * Translate listing description if missing UK or RU, save to DB.
   * Returns true if translation was performed and saved.
   */
  private async translateIfNeeded(listing: UnifiedListing): Promise<boolean> {
    if (!listing.description) return false;

    const hasUk = !!listing.description.uk?.trim();
    const hasRu = !!listing.description.ru?.trim();

    // Both present or neither — nothing to do
    if ((hasUk && hasRu) || (!hasUk && !hasRu)) return false;

    const translated = await this.translationService.ensureTranslations(listing);
    if (!translated || translated === listing.description) return false;

    // Check if translation actually added something new
    const newUk = translated.uk?.trim();
    const newRu = translated.ru?.trim();
    if (newUk === listing.description.uk?.trim() && newRu === listing.description.ru?.trim()) {
      return false;
    }

    // Save translation to DB
    listing.description = translated;
    await this.dataSource.query(
      `UPDATE unified_listings SET description = $1 WHERE id = $2`,
      [JSON.stringify(translated), listing.id],
    );

    return true;
  }

  /**
   * For OLX listings: re-resolve street from text (coordinates are unreliable).
   * Updates listing.streetId and saves to DB if a better match is found.
   */
  private async fixOlxStreet(listing: UnifiedListing): Promise<void> {
    if (listing.realtyPlatform !== 'olx' || !listing.geoId) return;

    const text = this.buildTextForStreetMatching(listing);
    if (!text) return;

    const streetResult = await this.streetMatcherService.resolveStreetByText(text, listing.geoId);
    if (!streetResult.streetId) return;

    // Only update if different from current
    if (streetResult.streetId !== listing.streetId) {
      this.logger.log(
        `OLX street fix: sourceId=${listing.sourceId} streetId ${listing.streetId || 'null'} → ${streetResult.streetId} (${streetResult.matchMethod}, confidence=${streetResult.confidence.toFixed(2)})`,
      );
      listing.streetId = streetResult.streetId;
      await this.dataSource.query(
        'UPDATE unified_listings SET street_id = $1 WHERE id = $2',
        [streetResult.streetId, listing.id],
      );
    }
  }

  /** Build text for street matching from listing's primaryData and description */
  private buildTextForStreetMatching(listing: UnifiedListing): string {
    const parts: string[] = [];
    const pd = (listing as any).primaryData;

    if (pd) {
      // OLX: location.pathName (e.g. "Київ > Печерський > вул. Грушевського")
      if (pd.location && typeof pd.location === 'object') {
        if (pd.location.pathName) parts.push(String(pd.location.pathName));
      }
      // Title often contains address
      if (pd.title) parts.push(String(pd.title));
    }

    // Description
    if (listing.description?.uk) {
      parts.push(listing.description.uk);
    }

    return parts.join(' ');
  }

  /**
   * Process items with limited concurrency.
   * Runs up to `this.concurrency` tasks in parallel with optional throttle delay.
   */
  private async processWithConcurrency<T>(
    items: T[],
    handler: (item: T) => Promise<void>,
  ): Promise<void> {
    let idx = 0;
    const workers = Array.from({ length: Math.min(this.concurrency, items.length) }, async () => {
      while (idx < items.length) {
        const i = idx++;
        await handler(items[i]);
        if (this.throttleMs > 0) {
          await new Promise(r => setTimeout(r, this.throttleMs));
        }
      }
    });
    await Promise.all(workers);
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
