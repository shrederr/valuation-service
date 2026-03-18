import { Controller, Get, Post, Param, Body, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiQuery } from '@nestjs/swagger';
import { ExportService } from './export.service';
import { ExportRunDto, ExportByPlatformsDto } from './dto';

@ApiTags('Export')
@Controller('api/v1/export')
export class ExportController {
  constructor(private readonly exportService: ExportService) {}

  @Post('run')
  @ApiOperation({ summary: 'Trigger export manually' })
  async run(@Body() body: ExportRunDto) {
    return this.exportService.runExport({ batchSize: body.batchSize, geoId: body.geoId, realtyType: body.realtyType });
  }

  @Get('status')
  @ApiOperation({ summary: 'Get export status' })
  getStatus() {
    return this.exportService.getStatus();
  }

  @Get('stats')
  @ApiOperation({ summary: 'Get export statistics' })
  @ApiResponse({ status: 200 })
  async getStats() {
    return this.exportService.getStats();
  }

  @Get('preview/:id')
  @ApiOperation({ summary: 'Preview export mapping for a listing' })
  async preview(@Param('id') id: string) {
    const result = await this.exportService.previewExport(id);
    if (!result) {
      return { error: 'Listing not found' };
    }
    return result;
  }

  @Post('single/:id')
  @ApiOperation({ summary: 'Export a single listing to CRM' })
  async exportSingle(@Param('id') id: string) {
    return this.exportService.exportSingle(id);
  }

  @Post('run-by-platforms')
  @ApiOperation({ summary: 'Export N objects per platform (balanced export)' })
  async runByPlatforms(@Body() body: ExportByPlatformsDto) {
    const platforms = body.platforms || ['olx', 'realtorUa', 'domRia', 'mlsUkraine'];
    const perPlatform = body.perPlatform || 25;
    return this.exportService.exportByPlatforms({
      platforms,
      perPlatform,
      realtyType: body.realtyType,
    });
  }

  @Post('translate-batch')
  @ApiOperation({ summary: 'Pre-translate descriptions (UK↔RU) for listings missing a language' })
  @ApiQuery({ name: 'batchSize', required: false, type: Number })
  async translateBatch(@Query('batchSize') batchSize?: string) {
    return this.exportService.translateBatch(batchSize ? Number(batchSize) : 100);
  }

  @Post('translate-all')
  @ApiOperation({ summary: 'Continuous translation of all objects missing RU/UK descriptions (uses Google Translate fallback)' })
  async translateAll(@Body() body?: { batchSize?: number; concurrency?: number; throttleMs?: number }) {
    const promise = this.exportService.translateAll(
      body?.batchSize || 200,
      body?.concurrency || 3,
      body?.throttleMs || 300,
    );
    promise.catch((err) => console.error('translateAll error:', err));
    return { started: true, message: 'Translate all started. Check GET /api/v1/export/progress for status.' };
  }

  @Post('batch-resolve-streets')
  @ApiOperation({ summary: 'Batch re-resolve streets for non-OLX listings using text matching' })
  async batchResolveStreets(@Body() body?: { batchSize?: number; platforms?: string[] }) {
    return this.exportService.batchResolveStreets({
      batchSize: body?.batchSize || 500,
      platforms: body?.platforms,
    });
  }

  @Post('run-all')
  @ApiOperation({ summary: 'Continuous export: loops until all pending objects are exported' })
  async runAll(@Body() body?: { batchSize?: number; concurrency?: number; platforms?: string[]; skipDedup?: boolean }) {
    // Fire and forget — runs in background, check progress via GET /progress
    const promise = this.exportService.runAll({
      batchSize: body?.batchSize || 1000,
      concurrency: body?.concurrency || 10,
      platforms: body?.platforms,
      skipDedup: body?.skipDedup || false,
    });
    // Don't await — return immediately so HTTP doesn't timeout
    promise.catch((err) => console.error('runAll error:', err));
    return {
      started: true,
      skipDedup: body?.skipDedup || false,
      message: `Full export started${body?.skipDedup ? ' (dedup skipped)' : ''}. Check GET /api/v1/export/progress for status.`,
    };
  }

  @Post('pre-dedup')
  @ApiOperation({ summary: 'Batch dedup check: marks all pending duplicates without exporting (DB only, no CRM calls)' })
  async preDedup(@Body() body?: { batchSize?: number; concurrency?: number; platforms?: string[] }) {
    const promise = this.exportService.preDedup({
      batchSize: body?.batchSize || 1000,
      concurrency: body?.concurrency || 10,
      platforms: body?.platforms,
    });
    promise.catch((err) => console.error('preDedup error:', err));
    return { started: true, message: 'Pre-dedup started. Check GET /api/v1/export/progress for status.' };
  }

  @Post('resend-all')
  @ApiOperation({ summary: 'Re-send all exported objects to CRM (triggers site sync via handleUpdate)' })
  async resendAll(@Body() body?: { batchSize?: number; concurrency?: number }) {
    const promise = this.exportService.resendAll({
      batchSize: body?.batchSize || 500,
      concurrency: body?.concurrency || 10,
    });
    promise.catch((err) => console.error('resendAll error:', err));
    return { started: true, message: 'Resend all started. Check GET /api/v1/export/progress for status.' };
  }

  @Post('stop')
  @ApiOperation({ summary: 'Stop running export after current batch' })
  async stop() {
    return this.exportService.stopExport();
  }

  @Get('progress')
  @ApiOperation({ summary: 'Get progress of run-all export' })
  getProgress() {
    return this.exportService.getRunAllProgress();
  }

  @Post('deactivate')
  @ApiOperation({ summary: 'Archive exported objects that are no longer active on source platforms' })
  async deactivate() {
    return this.exportService.deactivateDeletedObjects();
  }

  @Post('deactivate-region/:geoId')
  @ApiOperation({ summary: 'Deactivate all exported objects in a region (and sub-geos) from CRM' })
  async deactivateRegion(@Param('geoId') geoId: string, @Body() body?: { batchSize?: number }) {
    const promise = this.exportService.deactivateByRegion(Number(geoId), body?.batchSize || 200);
    promise.catch((err) => console.error('deactivateByRegion error:', err));
    return { started: true, regionGeoId: Number(geoId), message: `Deactivation by region started. Check GET /api/v1/export/progress for status.` };
  }
}
