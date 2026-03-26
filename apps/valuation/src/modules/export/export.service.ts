import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { DataSource } from 'typeorm';
import { UnifiedListing } from '@libs/database';
import { DedupCheckService } from './services/dedup-check.service';
import { TranslationService } from './services/translation.service';
import { PhotoDedupService } from './services/photo-dedup.service';
import { PrimaryDataExtractor } from './services/primary-data-extractor';
import { ToCrmMapper } from './mappers/to-crm.mapper';
import { CrmClientService } from './services/crm-client.service';
import { StreetMatcherService } from '../osm/street-matcher.service';
import { ExportStatsDto } from './dto';

/** Geo IDs of regions excluded from export (Odessa region + all children resolved at startup) */
const EXCLUDED_REGION_IDS = [18263, 20479]; // Одеська та Львівська області

@Injectable()
export class ExportService {
  private readonly logger = new Logger(ExportService.name);
  private readonly batchSize: number;
  private readonly concurrency: number;
  private readonly throttleMs: number;
  private readonly enabled: boolean;
  private running = false;
  private runStartedAt: Date | null = null;
  private readonly LOCK_TIMEOUT_MS = 4 * 60 * 60 * 1000; // 4 hours for full export
  private lastRunAt: Date | null = null;
  private lastRunStats: { exported: number; duplicates: number; errors: number; skipped: number } | null = null;
  private stopRequested = false;
  private runAllProgress: { exported: number; duplicates: number; errors: number; skipped: number; elapsed: string; batchNum: number } | null = null;
  private excludedGeoIds: Set<number> = new Set();

  constructor(
    private readonly dataSource: DataSource,
    private readonly configService: ConfigService,
    private readonly dedupCheckService: DedupCheckService,
    private readonly translationService: TranslationService,
    private readonly streetMatcherService: StreetMatcherService,
    private readonly toCrmMapper: ToCrmMapper,
    private readonly crmClientService: CrmClientService,
    private readonly photoDedupService: PhotoDedupService,
    private readonly primaryDataExtractor: PrimaryDataExtractor,
  ) {
    this.batchSize = parseInt(this.configService.get('EXPORT_BATCH_SIZE', '500'), 10);
    this.concurrency = parseInt(this.configService.get('EXPORT_CONCURRENCY', '5'), 10);
    this.throttleMs = parseInt(this.configService.get('EXPORT_THROTTLE_MS', '50'), 10);
    this.enabled = this.configService.get('EXPORT_ENABLED', 'false') === 'true';
    // Load excluded geo IDs (region + all children) at startup
    this.loadExcludedGeoIds();
  }

  private async loadExcludedGeoIds() {
    if (EXCLUDED_REGION_IDS.length === 0) return;
    try {
      const rows: { id: number }[] = await this.dataSource.query(`
        WITH RECURSIVE excluded AS (
          SELECT id FROM geo WHERE id = ANY($1::int[])
          UNION ALL
          SELECT g.id FROM geo g JOIN excluded e ON g.parent_id = e.id
        )
        SELECT id FROM excluded
      `, [EXCLUDED_REGION_IDS]);
      this.excludedGeoIds = new Set(rows.map(r => r.id));
      this.logger.log(`Loaded ${this.excludedGeoIds.size} excluded geo IDs for regions: ${EXCLUDED_REGION_IDS.join(', ')}`);
    } catch (err) {
      this.logger.error(`Failed to load excluded geo IDs: ${err}`);
    }
  }

  private getExcludedGeoFilter(paramIdx: number): { sql: string; param: number[] | null; nextIdx: number } {
    if (this.excludedGeoIds.size === 0) return { sql: '', param: null, nextIdx: paramIdx };
    return {
      sql: `AND geo_id NOT IN (SELECT unnest($${paramIdx}::int[]))`,
      param: Array.from(this.excludedGeoIds),
      nextIdx: paramIdx + 1,
    };
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

  /**
   * Deactivate all exported objects belonging to a specific region (and all sub-geos).
   * Sends deactivation to CRM and marks as 'deactivated' in local DB.
   */
  async deactivateByRegion(regionGeoId: number, batchSize = 200): Promise<{ total: number; deactivated: number; errors: number }> {
    // Resolve all geo IDs in the region
    const geoRows: { id: number }[] = await this.dataSource.query(`
      WITH RECURSIVE region_geos AS (
        SELECT id FROM geo WHERE id = $1
        UNION ALL
        SELECT g.id FROM geo g JOIN region_geos rg ON g.parent_id = rg.id
      )
      SELECT id FROM region_geos
    `, [regionGeoId]);
    const geoIds = geoRows.map(r => r.id);
    this.logger.log(`DeactivateByRegion: ${geoIds.length} geo IDs in region ${regionGeoId}`);

    // Find all exported objects in this region
    const ids: { id: string }[] = await this.dataSource.query(`
      SELECT id FROM unified_listings
      WHERE export_status = 'exported'
        AND crm_external_id IS NOT NULL
        AND geo_id = ANY($1::int[])
    `, [geoIds]);

    this.logger.log(`DeactivateByRegion: ${ids.length} exported objects to deactivate`);
    if (!ids.length) return { total: 0, deactivated: 0, errors: 0 };

    let deactivated = 0;
    let errors = 0;

    for (let i = 0; i < ids.length; i += batchSize) {
      const batch = ids.slice(i, i + batchSize);

      for (const { id } of batch) {
        if (this.stopRequested) break;
        try {
          const rows = await this.dataSource.query(`SELECT * FROM unified_listings WHERE id = $1`, [id]);
          if (!rows.length) { errors++; continue; }
          const listing = this.hydrate(rows[0]);

          const dto = await this.toCrmMapper.map(listing);
          (dto as any).deleted_at = new Date().toISOString();

          const result = await this.crmClientService.importObject(dto);
          if (result.success) {
            await this.dataSource.query(
              `UPDATE unified_listings SET export_status = 'deactivated' WHERE id = $1`, [id],
            );
            deactivated++;
          } else {
            this.logger.warn(`DeactivateByRegion failed: ${listing.sourceId}: ${result.error}`);
            errors++;
          }
        } catch (err) {
          this.logger.error(`DeactivateByRegion error: ${id}: ${err}`);
          errors++;
        }
      }

      this.runAllProgress = {
        exported: deactivated, duplicates: 0, errors, skipped: 0,
        elapsed: this.formatElapsed(Date.now() - Date.now()), batchNum: Math.floor(i / batchSize) + 1,
      };
      this.logger.log(`DeactivateByRegion progress: ${deactivated}/${ids.length} deactivated, ${errors} errors`);
    }

    this.logger.log(`DeactivateByRegion complete: ${deactivated} deactivated, ${errors} errors out of ${ids.length}`);
    return { total: ids.length, deactivated, errors };
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

  /**
   * Continuous export: loops until all pending objects are exported or stopExport() is called.
   * Uses higher concurrency and no throttle for maximum throughput.
   */
  /**
   * Pre-dedup: batch check all pending objects for duplicates WITHOUT exporting.
   * Marks duplicates in DB (export_status='duplicate') so subsequent export can skip dedup.
   */
  async preDedup(opts?: {
    batchSize?: number;
    concurrency?: number;
    platforms?: string[];
  }): Promise<{ checked: number; duplicates: number; errors: number; batches: number; elapsed: string }> {
    if (!this.acquireLock()) {
      this.logger.warn('Export already running, skipping preDedup');
      return { checked: 0, duplicates: 0, errors: 0, batches: 0, elapsed: '0s' };
    }

    const batchSize = opts?.batchSize || 1000;
    const concurrencyOverride = opts?.concurrency || 10;
    const platforms = opts?.platforms;
    const startTime = Date.now();
    this.stopRequested = false;

    const total = { checked: 0, duplicates: 0, errors: 0 };
    let batchNum = 0;

    try {
      this.logger.log(`=== PRE-DEDUP STARTED (batch=${batchSize}, concurrency=${concurrencyOverride}) ===`);

      while (!this.stopRequested) {
        batchNum++;

        let platformFilter = '';
        const params: unknown[] = [];
        let paramIdx = 1;

        if (platforms?.length) {
          platformFilter = `AND realty_platform = ANY($${paramIdx})`;
          params.push(platforms);
          paramIdx++;
        }

        // Exclude regions (e.g. Odessa)
        let excludeFilter = '';
        if (this.excludedGeoIds.size > 0) {
          excludeFilter = `AND geo_id NOT IN (SELECT unnest($${paramIdx}::int[]))`;
          params.push(Array.from(this.excludedGeoIds));
          paramIdx++;
        }

        params.push(batchSize);

        const listings: Record<string, unknown>[] = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE source_type = 'aggregator'
             AND is_active = true
             AND export_status IS NULL
             AND deleted_at IS NULL
             AND price > 0 AND total_area > 0 AND geo_id IS NOT NULL
             ${platformFilter}
             ${excludeFilter}
           ORDER BY published_at DESC NULLS LAST
           LIMIT $${paramIdx}`,
          params,
        );

        if (listings.length === 0) {
          this.logger.log('No more pending listings — pre-dedup complete');
          break;
        }

        const batchStats = { checked: 0, duplicates: 0, errors: 0 };

        await this.processWithConcurrencyOverride(listings, concurrencyOverride, async (raw) => {
          if (this.stopRequested) return;
          try {
            const listing = this.hydrate(raw);

            const dedup = await this.dedupCheckService.isDuplicate(listing);
            if (dedup.isDuplicate) {
              await this.updateExportStatus(listing.id, 'duplicate', null,
                `Matched ${dedup.matchLevel}: ${dedup.matchedId}${dedup.similarity ? ` (sim=${dedup.similarity.toFixed(3)})` : ''}`);
              batchStats.duplicates++;
            }
            batchStats.checked++;
          } catch (err) {
            batchStats.errors++;
            this.logger.warn(`Pre-dedup error for ${raw.id}: ${err instanceof Error ? err.message : err}`);
          }
        });

        total.checked += batchStats.checked;
        total.duplicates += batchStats.duplicates;
        total.errors += batchStats.errors;

        const elapsed = this.formatElapsed(Date.now() - startTime);
        const rate = Math.round(total.checked / ((Date.now() - startTime) / 60000));
        this.runAllProgress = {
          exported: 0, duplicates: total.duplicates, errors: total.errors,
          skipped: total.checked - total.duplicates, elapsed, batchNum,
        };

        this.logger.log(
          `Pre-dedup batch #${batchNum}: +${batchStats.checked} checked, +${batchStats.duplicates} dup | ` +
          `Total: ${total.checked} checked, ${total.duplicates} dup | ${elapsed} | ~${rate}/min`,
        );
      }
    } finally {
      this.releaseLock();
      this.stopRequested = false;
    }

    const elapsed = this.formatElapsed(Date.now() - startTime);
    this.logger.log(`=== PRE-DEDUP DONE: ${total.checked} checked, ${total.duplicates} duplicates in ${elapsed} (${batchNum} batches) ===`);
    return { ...total, batches: batchNum, elapsed };
  }

  /**
   * Resend all exported objects to CRM as updates.
   * This triggers handleUpdate() path on CRM side → PushToQueueBehaviour → site sync.
   */
  async resendAll(opts?: {
    batchSize?: number;
    concurrency?: number;
  }): Promise<{ resent: number; errors: number; batches: number; elapsed: string }> {
    if (!this.acquireLock()) {
      this.logger.warn('Export already running, skipping resendAll');
      return { resent: 0, errors: 0, batches: 0, elapsed: '0s' };
    }

    const batchSize = opts?.batchSize || 500;
    const concurrencyOverride = opts?.concurrency || 10;
    const startTime = Date.now();
    this.stopRequested = false;

    const total = { resent: 0, errors: 0 };
    let batchNum = 0;
    let offset = 0;

    try {
      this.logger.log(`=== RESEND ALL STARTED (batch=${batchSize}, concurrency=${concurrencyOverride}) ===`);

      while (!this.stopRequested) {
        batchNum++;

        const listings: Record<string, unknown>[] = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE export_status = 'exported'
             AND crm_external_id IS NOT NULL
             AND is_active = true
             AND deleted_at IS NULL
           ORDER BY last_exported_at ASC NULLS FIRST
           LIMIT $1 OFFSET $2`,
          [batchSize, offset],
        );

        if (listings.length === 0) {
          this.logger.log('No more exported listings — resend complete');
          break;
        }

        const batchStats = { resent: 0, errors: 0 };

        await this.processWithConcurrencyOverride(listings, concurrencyOverride, async (raw) => {
          if (this.stopRequested) return;
          try {
            const listing = this.hydrate(raw);
            await this.translateIfNeeded(listing);

            const dto = await this.toCrmMapper.map(listing);
            const result = await this.crmClientService.importObject(dto);

            if (result.success) {
              batchStats.resent++;
            } else {
              batchStats.errors++;
              this.logger.warn(`Resend failed: sourceId=${listing.sourceId}: ${result.error}`);
            }
          } catch (err) {
            batchStats.errors++;
            this.logger.warn(`Resend error for ${raw.id}: ${err instanceof Error ? err.message : err}`);
          }
        });

        total.resent += batchStats.resent;
        total.errors += batchStats.errors;
        offset += listings.length;

        const elapsed = this.formatElapsed(Date.now() - startTime);
        const rate = Math.round(total.resent / ((Date.now() - startTime) / 60000));
        this.runAllProgress = {
          exported: total.resent, duplicates: 0, errors: total.errors,
          skipped: 0, elapsed, batchNum,
        };

        this.logger.log(
          `Resend batch #${batchNum}: +${batchStats.resent} sent, +${batchStats.errors} err | ` +
          `Total: ${total.resent} resent, ${total.errors} err | ${elapsed} | ~${rate}/min`,
        );
      }
    } finally {
      this.releaseLock();
      this.stopRequested = false;
    }

    const elapsed = this.formatElapsed(Date.now() - startTime);
    this.logger.log(`=== RESEND ALL DONE: ${total.resent} resent, ${total.errors} errors in ${elapsed} (${batchNum} batches) ===`);
    return { ...total, batches: batchNum, elapsed };
  }

  async runAll(opts?: {
    batchSize?: number;
    concurrency?: number;
    platforms?: string[];
    skipDedup?: boolean;
    skipTranslation?: boolean;
  }): Promise<{ exported: number; duplicates: number; errors: number; skipped: number; batches: number; elapsed: string }> {
    if (!this.acquireLock()) {
      this.logger.warn('Export already running, skipping runAll');
      return { exported: 0, duplicates: 0, errors: 0, skipped: 0, batches: 0, elapsed: '0s' };
    }

    const batchSize = opts?.batchSize || 1000;
    const concurrencyOverride = opts?.concurrency || 10;
    const platforms = opts?.platforms;
    const skipDedup = opts?.skipDedup || false;
    const skipTranslation = opts?.skipTranslation ?? skipDedup; // skip translation when skipDedup (fast mode)
    const startTime = Date.now();
    this.stopRequested = false;

    const total = { exported: 0, duplicates: 0, errors: 0, skipped: 0 };
    let batchNum = 0;

    try {
      this.logger.log(`=== FULL EXPORT STARTED (batch=${batchSize}, concurrency=${concurrencyOverride}, platforms=${platforms?.join(',') || 'all'}, skipDedup=${skipDedup}, skipTranslation=${skipTranslation}) ===`);

      while (!this.stopRequested) {
        batchNum++;

        // Build platform filter
        let platformFilter = '';
        const params: unknown[] = [];
        let paramIdx = 1;

        if (platforms?.length) {
          platformFilter = `AND realty_platform = ANY($${paramIdx})`;
          params.push(platforms);
          paramIdx++;
        }

        // Exclude regions (e.g. Odessa)
        let excludeFilter = '';
        if (this.excludedGeoIds.size > 0) {
          excludeFilter = `AND geo_id NOT IN (SELECT unnest($${paramIdx}::int[]))`;
          params.push(Array.from(this.excludedGeoIds));
          paramIdx++;
        }

        params.push(batchSize);

        const listings: Record<string, unknown>[] = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE source_type = 'aggregator'
             AND is_active = true
             AND export_status IS NULL
             AND deleted_at IS NULL
             AND price > 0 AND total_area > 0 AND geo_id IS NOT NULL
             ${platformFilter}
             ${excludeFilter}
           ORDER BY published_at DESC NULLS LAST
           LIMIT $${paramIdx}`,
          params,
        );

        if (listings.length === 0) {
          this.logger.log('No more pending listings — export complete');
          break;
        }

        const batchStats = { exported: 0, duplicates: 0, errors: 0, skipped: 0 };

        // Use higher concurrency, no throttle
        await this.processWithConcurrencyOverride(listings, concurrencyOverride, async (raw) => {
          if (this.stopRequested) return;
          try {
            const listing = this.hydrate(raw);

            // Dedup check (skippable if pre-dedup was already run)
            if (!skipDedup) {
              const dedup = await this.dedupCheckService.isDuplicate(listing);
              if (dedup.isDuplicate) {
                await this.updateExportStatus(listing.id, 'duplicate', null,
                  `Matched ${dedup.matchLevel}: ${dedup.matchedId}${dedup.similarity ? ` (sim=${dedup.similarity.toFixed(3)})` : ''}`);
                batchStats.duplicates++;
                return;
              }
            }

            if (!skipTranslation) {
              await this.translateIfNeeded(listing);
            }
            await this.fixOlxStreet(listing);
            await this.fixStreetByText(listing);

            const dto = await this.toCrmMapper.map(listing);

            if (listing.realtyType === 'apartment' && !dto.attributes?.square_living) {
              await this.updateExportStatus(listing.id, 'skipped', null, 'Missing square_living');
              batchStats.skipped++;
              return;
            }

            const result = await this.crmClientService.importObject(dto);
            if (!result.success) {
              await this.updateExportStatus(listing.id, 'error', null, result.error);
              batchStats.errors++;
              return;
            }
            await this.updateExportStatus(listing.id, 'exported', result.id || null);
            batchStats.exported++;
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            await this.updateExportStatus(raw.id as string, 'error', null, msg);
            batchStats.errors++;
          }
        });

        total.exported += batchStats.exported;
        total.duplicates += batchStats.duplicates;
        total.errors += batchStats.errors;
        total.skipped += batchStats.skipped;

        const elapsed = this.formatElapsed(Date.now() - startTime);
        const rate = Math.round(total.exported / ((Date.now() - startTime) / 60000));
        this.runAllProgress = { ...total, elapsed, batchNum };

        this.logger.log(
          `Batch #${batchNum}: +${batchStats.exported} exported, +${batchStats.duplicates} dup, +${batchStats.errors} err | ` +
          `Total: ${total.exported} exported, ${total.duplicates} dup, ${total.errors} err | ${elapsed} | ~${rate}/min`,
        );
      }
    } finally {
      this.releaseLock();
      this.stopRequested = false;
      this.lastRunStats = { exported: total.exported, duplicates: total.duplicates, errors: total.errors, skipped: total.skipped };
    }

    const elapsed = this.formatElapsed(Date.now() - startTime);
    this.logger.log(`=== FULL EXPORT DONE: ${total.exported} exported, ${total.duplicates} dup, ${total.errors} err in ${elapsed} (${batchNum} batches) ===`);
    return { ...total, batches: batchNum, elapsed };
  }

  stopExport(): { stopped: boolean; progress: typeof this.runAllProgress } {
    if (!this.running) {
      return { stopped: false, progress: null };
    }
    this.stopRequested = true;
    this.logger.log('Stop requested — will stop after current batch');
    return { stopped: true, progress: this.runAllProgress };
  }

  getRunAllProgress() {
    return {
      running: this.running,
      stopRequested: this.stopRequested,
      progress: this.runAllProgress,
    };
  }

  private formatElapsed(ms: number): string {
    const s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    return h > 0 ? `${h}h${m}m` : `${m}m${s % 60}s`;
  }

  /**
   * Process items with custom concurrency and NO throttle (for bulk export).
   */
  private async processWithConcurrencyOverride<T>(
    items: T[],
    concurrency: number,
    handler: (item: T) => Promise<void>,
  ): Promise<void> {
    let idx = 0;
    const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (idx < items.length) {
        const i = idx++;
        if (i >= items.length) break;
        await handler(items[i]);
      }
    });
    await Promise.all(workers);
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

    // Exclude regions (e.g. Odessa)
    const geoFilter = this.getExcludedGeoFilter(paramIdx);
    if (geoFilter.param) {
      conditions.push(`geo_id NOT IN (SELECT unnest($${paramIdx}::int[]))`);
      params.push(geoFilter.param);
      paramIdx = geoFilter.nextIdx;
    }

    params.push(limit);
    const limitParam = `$${paramIdx}`;

    const listings: Record<string, unknown>[] = await this.dataSource.query(
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

        // Re-resolve street: OLX from text (coordinates unreliable), others via text fallback
        await this.fixOlxStreet(listing);
        await this.fixStreetByText(listing);

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
        await this.updateExportStatus(raw.id as string, 'error', null, msg);
        stats.errors++;
        this.logger.error(`Export error for ${raw.id}: ${msg}`);
      }
    });

    return stats;
  }

  private async processUpdatedObjects(limit: number) {
    const stats = { exported: 0, errors: 0 };

    const listings: Record<string, unknown>[] = await this.dataSource.query(
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
          await this.updateExportStatus(listing.id, 'error', raw.crm_external_id as string, result.error);
          stats.errors++;
          return;
        }
        await this.updateExportStatus(listing.id, 'exported', result.id || raw.crm_external_id as string);
        stats.exported++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await this.updateExportStatus(raw.id as string, 'error', raw.crm_external_id as string, msg);
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

        const listings: Record<string, unknown>[] = await this.dataSource.query(
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

            // Re-resolve street: OLX from text (coordinates unreliable), others via text fallback
            await this.fixOlxStreet(listing);
            await this.fixStreetByText(listing);

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
            await this.updateExportStatus(raw.id as string, 'error', null, msg);
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

    const listings = await this.dataSource.query(
      `SELECT * FROM unified_listings
       WHERE source_type = 'aggregator'
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
      if (this.stopRequested) break;
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
   * Continuous batch translation of all objects missing ru/uk descriptions.
   * Runs in background, can be stopped via stopExport().
   * Prioritizes exported objects first, then pending.
   */
  async translateAll(batchSize = 200, concurrency = 3, throttleMs = 300): Promise<void> {
    if (this.running) {
      this.logger.warn('Export/translation already running');
      return;
    }

    this.running = true;
    this.stopRequested = false;
    this.runStartedAt = new Date();
    this.runAllProgress = { exported: 0, duplicates: 0, errors: 0, skipped: 0, elapsed: '0m0s', batchNum: 0 };
    const stats = { translated: 0, skipped: 0, errors: 0 };

    this.logger.log(`=== translateAll started: batchSize=${batchSize}, concurrency=${concurrency}, throttleMs=${throttleMs} ===`);

    try {
      let batchNum = 0;
      while (!this.stopRequested) {
        batchNum++;

        // Prioritize exported objects first, then pending
        const listings = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE source_type = 'aggregator'
             AND is_active = true
             AND deleted_at IS NULL
             AND description IS NOT NULL
             AND (
               (description->>'uk' IS NOT NULL AND TRIM(description->>'uk') != '' AND (description->>'ru' IS NULL OR TRIM(description->>'ru') = ''))
               OR
               (description->>'ru' IS NOT NULL AND TRIM(description->>'ru') != '' AND (description->>'uk' IS NULL OR TRIM(description->>'uk') = ''))
             )
           ORDER BY
             CASE WHEN export_status = 'exported' THEN 0 ELSE 1 END,
             updated_at DESC
           LIMIT $1`,
          [batchSize],
        );

        if (listings.length === 0) {
          this.logger.log('translateAll: no more listings to translate');
          break;
        }

        this.logger.log(`translateAll batch #${batchNum}: ${listings.length} listings`);

        // Process with concurrency + throttle to avoid Google rate limiting
        let idx = 0;
        const workers = Array.from({ length: Math.min(concurrency, listings.length) }, async () => {
          while (idx < listings.length && !this.stopRequested) {
            const i = idx++;
            if (i >= listings.length) break;
            const raw = listings[i];
            try {
              const listing = this.hydrate(raw);
              const updated = await this.translateIfNeeded(listing);
              if (updated) stats.translated++;
              else stats.skipped++;
            } catch (err) {
              stats.errors++;
              this.logger.warn(`translateAll error for ${raw.id}: ${err instanceof Error ? err.message : err}`);
            }
            // Throttle to avoid rate limiting
            if (throttleMs > 0) {
              await new Promise(r => setTimeout(r, throttleMs));
            }
          }
        });
        await Promise.all(workers);

        // Update progress
        const elapsed = Date.now() - this.runStartedAt!.getTime();
        const mins = Math.floor(elapsed / 60000);
        const secs = Math.floor((elapsed % 60000) / 1000);
        this.runAllProgress = {
          exported: stats.translated,
          duplicates: 0,
          errors: stats.errors,
          skipped: stats.skipped,
          elapsed: `${mins}m${secs}s`,
          batchNum,
        };

        this.logger.log(`translateAll progress: translated=${stats.translated}, skipped=${stats.skipped}, errors=${stats.errors}, elapsed=${mins}m${secs}s`);
      }
    } finally {
      this.running = false;
      this.logger.log(`=== translateAll finished: translated=${stats.translated}, skipped=${stats.skipped}, errors=${stats.errors} ===`);
    }
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

    // Try matching in listing's geo first
    let streetResult = await this.streetMatcherService.resolveStreetByText(text, listing.geoId);

    // Fallback: if district-level geo didn't match, try parent city
    if (!streetResult.streetId) {
      const parentCity = await this.dataSource.query(
        `SELECT g2.id FROM geo g1
         JOIN geo g2 ON ST_Contains(g2.polygon, ST_Centroid(g1.polygon))
           AND g2.type IN ('city', 'village')
           AND g2.id != g1.id
         WHERE g1.id = $1
         LIMIT 1`,
        [listing.geoId],
      );
      if (parentCity.length > 0) {
        streetResult = await this.streetMatcherService.resolveStreetByText(text, parentCity[0].id);
      }
    }

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

      // domRia: street_name, street_name_uk
      if (pd.street_name_uk) parts.push(String(pd.street_name_uk));
      else if (pd.street_name) parts.push(String(pd.street_name));

      // realtorUa: address
      if (pd.address) parts.push(String(pd.address));

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
   * For non-OLX listings: re-resolve street from text when current is nearest-only.
   * Uses full text search across all streets in geo for better accuracy.
   */
  private async fixStreetByText(listing: UnifiedListing): Promise<void> {
    if (listing.realtyPlatform === 'olx' || !listing.geoId) return;

    const text = this.buildTextForStreetMatching(listing);
    if (!text) return;

    const streetResult = await this.streetMatcherService.resolveStreetByText(text, listing.geoId);
    if (!streetResult.streetId || streetResult.confidence < 0.7) return;

    if (streetResult.streetId !== listing.streetId) {
      this.logger.debug(
        `Street fix: sourceId=${listing.sourceId} (${listing.realtyPlatform}) streetId ${listing.streetId || 'null'} → ${streetResult.streetId} (confidence=${streetResult.confidence.toFixed(2)})`,
      );
      listing.streetId = streetResult.streetId;
      await this.dataSource.query(
        'UPDATE unified_listings SET street_id = $1 WHERE id = $2',
        [streetResult.streetId, listing.id],
      );
    }
  }

  /**
   * Batch re-resolve streets for all non-OLX aggregator listings using text matching.
   * Returns stats about how many were updated.
   */
  async batchResolveStreets(opts?: { batchSize?: number; platforms?: string[] }): Promise<{
    processed: number; updated: number; errors: number; byPlatform: Record<string, { processed: number; updated: number }>;
  }> {
    const batchSize = opts?.batchSize || 500;
    const platforms = opts?.platforms || ['realtorUa', 'domRia', 'realEstateLvivUa', 'mlsUkraine'];
    const stats = { processed: 0, updated: 0, errors: 0, byPlatform: {} as Record<string, { processed: number; updated: number }> };

    for (const platform of platforms) {
      const platformStats = { processed: 0, updated: 0 };
      stats.byPlatform[platform] = platformStats;
      let offset = 0;

      while (true) {
        const listings = await this.dataSource.query(
          `SELECT * FROM unified_listings
           WHERE source_type = 'aggregator'
             AND is_active = true
             AND deleted_at IS NULL
             AND geo_id IS NOT NULL
             AND realty_platform = $1
           ORDER BY id
           LIMIT $2 OFFSET $3`,
          [platform, batchSize, offset],
        );

        if (listings.length === 0) break;

        for (const raw of listings) {
          try {
            const listing = this.hydrate(raw);
            const text = this.buildTextForStreetMatching(listing);
            if (!text || !listing.geoId) {
              stats.processed++;
              platformStats.processed++;
              continue;
            }

            const streetResult = await this.streetMatcherService.resolveStreetByText(text, listing.geoId);
            if (streetResult.streetId && streetResult.confidence >= 0.7 && streetResult.streetId !== listing.streetId) {
              await this.dataSource.query(
                'UPDATE unified_listings SET street_id = $1 WHERE id = $2',
                [streetResult.streetId, listing.id],
              );
              stats.updated++;
              platformStats.updated++;
            }
            stats.processed++;
            platformStats.processed++;
          } catch (err) {
            stats.errors++;
            this.logger.warn(`batchResolveStreets error: ${err instanceof Error ? err.message : err}`);
          }
        }

        offset += batchSize;
        this.logger.log(`batchResolveStreets [${platform}]: processed=${platformStats.processed}, updated=${platformStats.updated}, offset=${offset}`);
      }
    }

    this.logger.log(`batchResolveStreets done: processed=${stats.processed}, updated=${stats.updated}, errors=${stats.errors}`);
    return stats;
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

  /**
   * Test photo dedup on N objects: find relaxed geo candidates and compare via GPT-4o.
   * Returns detailed results for manual quality review.
   */
  async photoDedupTest(limit = 100): Promise<{
    total: number;
    results: Array<Record<string, unknown>>;
    stats: { same: number; different: number; uncertain: number; error: number; totalCostUsd: string; avgTimeMs: number };
  }> {
    this.logger.log(`PhotoDedupTest: starting test with limit=${limit}`);

    // Step 1: Get a pool of active aggregator objects with coordinates (lightweight query)
    const pool = await this.dataSource.query(`
      SELECT a.*, g.name::text as geo_name
      FROM unified_listings a
      LEFT JOIN geo g ON g.id = a.geo_id
      WHERE a.source_type = 'aggregator'
        AND a.is_active = true
        AND a.lat IS NOT NULL AND a.lng IS NOT NULL
        AND (a.total_area IS NOT NULL OR a.land_area IS NOT NULL)
        AND a.price > 0
      ORDER BY random()
      LIMIT $1
    `, [limit * 5]); // fetch 5x more since many won't have geo candidates

    this.logger.log(`PhotoDedupTest: fetched ${pool.length} candidate objects from pool`);

    // Step 2: For each, check if there are relaxed geo candidates (one at a time, fast)
    const candidates: typeof pool = [];
    for (const raw of pool) {
      if (candidates.length >= limit) break;
      const hasCand = await this.dataSource.query(`
        SELECT 1 FROM unified_listings b
        WHERE b.id != $1
          AND (b.source_type IN ('vector', 'vector_crm') OR (b.source_type = 'aggregator' AND b.export_status = 'exported'))
          AND (b.is_active = true OR b.source_type IN ('vector', 'vector_crm'))
          AND b.realty_type = $2
          AND b.lat IS NOT NULL AND b.lng IS NOT NULL
          AND ST_DWithin(
            geography(ST_SetSRID(ST_MakePoint(b.lng::float8, b.lat::float8), 4326)),
            geography(ST_SetSRID(ST_MakePoint($3::float8, $4::float8), 4326)),
            100
          )
          AND ($5::int IS NULL OR b.rooms = $5)
          AND b.price > 0 AND $6::numeric > 0
          AND ABS(b.price - $6) / NULLIF(GREATEST(b.price, $6), 0) <= 0.15
        LIMIT 1
      `, [raw.id, raw.realty_type, raw.lng, raw.lat, raw.rooms, raw.price]);
      if (hasCand.length > 0) candidates.push(raw);
    }

    this.logger.log(`PhotoDedupTest: found ${candidates.length} objects with relaxed geo candidates`);

    const results: Array<Record<string, unknown>> = [];
    const stats = { same: 0, different: 0, uncertain: 0, error: 0, totalTimeMs: 0 };

    for (let i = 0; i < candidates.length; i++) {
      const raw = candidates[i];
      const listing = this.hydrate(raw);

      // Find the relaxed geo candidate
      const candidateRows = await this.dataSource.query(`
        SELECT b.id, b.source_id, b.source_type, b.crm_external_id, b.realty_platform,
               b.price, b.total_area, b.land_area, b.rooms, b.lat, b.lng, b.external_url,
               ST_Distance(
                 geography(ST_SetSRID(ST_MakePoint(b.lng::float8, b.lat::float8), 4326)),
                 geography(ST_SetSRID(ST_MakePoint($2::float8, $3::float8), 4326))
               ) as distance_m
        FROM unified_listings b
        WHERE b.id != $1
          AND (b.source_type IN ('vector', 'vector_crm') OR (b.source_type = 'aggregator' AND b.export_status = 'exported'))
          AND (b.is_active = true OR b.source_type IN ('vector', 'vector_crm'))
          AND b.realty_type = $4
          AND b.lat IS NOT NULL AND b.lng IS NOT NULL
          AND ST_DWithin(
            geography(ST_SetSRID(ST_MakePoint(b.lng::float8, b.lat::float8), 4326)),
            geography(ST_SetSRID(ST_MakePoint($2::float8, $3::float8), 4326)),
            100
          )
          AND ($5::int IS NULL OR b.rooms = $5)
          AND b.price > 0 AND $6::numeric > 0
          AND ABS(b.price - $6) / NULLIF(GREATEST(b.price, $6), 0) <= 0.15
        ORDER BY distance_m
        LIMIT 1
      `, [listing.id, listing.lng, listing.lat, listing.realtyType, listing.rooms ?? null, listing.price ?? 0]);

      if (candidateRows.length === 0) continue;

      const cand = candidateRows[0];
      const candidateListing = await this.photoDedupService.loadListing(cand.id);
      if (!candidateListing) continue;

      // Extract photos for logging
      const listingData = this.primaryDataExtractor.extractForExport(listing);
      const candidateData = this.primaryDataExtractor.extractForExport(candidateListing);

      const startTime = Date.now();
      const photoResult = await this.photoDedupService.compare(listing, candidateListing);
      const elapsed = Date.now() - startTime;
      stats.totalTimeMs += elapsed;

      if (photoResult.verdict === 'SAME') stats.same++;
      else if (photoResult.verdict === 'DIFFERENT') stats.different++;
      else if (photoResult.verdict === 'UNCERTAIN') stats.uncertain++;
      else stats.error++;

      results.push({
        index: i + 1,
        listingId: listing.id,
        sourceId: raw.source_id,
        platform: raw.realty_platform,
        candidateId: cand.id,
        candidateCrmId: cand.crm_external_id,
        candidateSource: cand.source_type,
        candidatePlatform: cand.realty_platform,
        verdict: photoResult.verdict,
        confidence: photoResult.confidence,
        reasoning: photoResult.reasoning,
        listingPhotos: (listingData.photos || []).slice(0, 4),
        candidatePhotos: (candidateData.photos || []).slice(0, 4),
        listingUrl: listingData.url,
        candidateUrl: candidateData.url,
        geo: raw.geo_name,
        area: raw.total_area,
        price: raw.price,
        candidateArea: cand.total_area,
        candidatePrice: cand.price,
        distance_m: Math.round(parseFloat(cand.distance_m)),
        timeMs: elapsed,
      });

      this.logger.log(
        `PhotoDedupTest [${i + 1}/${candidates.length}]: ${raw.source_id} vs ${cand.crm_external_id || cand.source_id} ` +
        `→ ${photoResult.verdict} (${photoResult.confidence.toFixed(2)}) ${elapsed}ms`,
      );
    }

    const totalProcessed = stats.same + stats.different + stats.uncertain + stats.error;
    const costPerCall = 0.003;

    return {
      total: results.length,
      results,
      stats: {
        same: stats.same,
        different: stats.different,
        uncertain: stats.uncertain,
        error: stats.error,
        totalCostUsd: `$${(totalProcessed * costPerCall).toFixed(2)}`,
        avgTimeMs: totalProcessed > 0 ? Math.round(stats.totalTimeMs / totalProcessed) : 0,
      },
    };
  }
}
